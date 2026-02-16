#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

MASTER_CSV = DATA_DIR / "master_facilities.csv"
STATION_MISSES_CSV = DATA_DIR / "station_misses.csv"

# ---- env ----
API_KEY = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()

WARD_FILTER = (os.getenv("WARD_FILTER") or "").strip() or None
MAX_UPDATES = int(os.getenv("MAX_UPDATES", "999999"))  # デフォルト全件
ONLY_BAD_ROWS = (os.getenv("ONLY_BAD_ROWS", "0") == "1")
STRICT_ADDRESS_CHECK = (os.getenv("STRICT_ADDRESS_CHECK", "1") == "1")

SLEEP_SEC = float(os.getenv("GOOGLE_API_SLEEP_SEC", "0.15"))

OVERWRITE_PHONE = (os.getenv("OVERWRITE_PHONE", "0") == "1")
OVERWRITE_WEBSITE = (os.getenv("OVERWRITE_WEBSITE", "0") == "1")
OVERWRITE_MAP_URL = (os.getenv("OVERWRITE_MAP_URL", "0") == "1")

# station/walk
FILL_NEAREST_STATION = (os.getenv("FILL_NEAREST_STATION", "1") == "1")

# ★全件やり直し（駅/徒歩）
FORCE_RECALC_STATION = (os.getenv("FORCE_RECALC_STATION", "0") == "1")

# 通常は入力 overwrite_station_walk (=1) を workflow から渡す想定
OVERWRITE_NEAREST_STATION = (os.getenv("OVERWRITE_NEAREST_STATION", "0") == "1") or FORCE_RECALC_STATION
OVERWRITE_WALK_MINUTES = (os.getenv("OVERWRITE_WALK_MINUTES", "0") == "1") or FORCE_RECALC_STATION

STATION_RADIUS_M = int(os.getenv("STATION_RADIUS_M", "2000"))
STATION_CANDIDATES = int(os.getenv("STATION_CANDIDATES", "8"))

# 任意：lat/lng の上書き（ズレ対策したいときだけ 1）
OVERWRITE_LATLNG = (os.getenv("OVERWRITE_LATLNG", "0") == "1")


def norm(s: Any) -> str:
    if s is None:
        return ""
    x = str(s).replace("　", " ")
    x = re.sub(r"\s+", " ", x).strip()
    return x


def to_float(s: Any) -> Optional[float]:
    if s is None:
        return None
    t = str(s).strip()
    if t == "":
        return None
    try:
        return float(t)
    except Exception:
        return None


def ok_address(addr: str, ward: Optional[str]) -> bool:
    if not STRICT_ADDRESS_CHECK:
        return True
    if not addr:
        return False
    if "横浜市" not in addr:
        return False
    if ward and ward not in addr:
        return False
    return True


def require_api_key() -> None:
    if not API_KEY:
        raise RuntimeError("GOOGLE_MAPS_API_KEY が未設定です（GitHub Secretsに設定してください）")


def maps_get(url: str, params: Dict[str, str], timeout: int = 30) -> Dict[str, Any]:
    require_api_key()
    p = dict(params)
    p["key"] = API_KEY
    r = requests.get(url, params=p, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    status = data.get("status")
    if status not in ("OK", "ZERO_RESULTS"):
        raise RuntimeError(f"Google API error: status={status} error_message={data.get('error_message')}")
    time.sleep(SLEEP_SEC)
    return data


def places_text_search(query: str) -> Optional[Dict[str, Any]]:
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    data = maps_get(url, {"query": query, "language": "ja", "region": "jp"})
    results = data.get("results") or []
    return results[0] if results else None


def place_details(place_id: str) -> Optional[Dict[str, Any]]:
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = ",".join(
        [
            "name",
            "formatted_address",
            "geometry/location",
            "url",
            "website",
            "formatted_phone_number",
            "types",
        ]
    )
    data = maps_get(url, {"place_id": place_id, "fields": fields, "language": "ja"})
    return (data.get("result") or None)


def nearby_stations(lat: float, lng: float) -> List[Tuple[str, str]]:
    """
    近傍の駅候補（name, place_id）を返す。駅以外が混ざるのを強く防ぐ。
    """
    # 駅として扱うタイプ（Googleのtypesは揺れる）
    STATION_TYPES = {
        "train_station",
        "subway_station",
        "transit_station",
        "light_rail_station",
    }

    # 駅じゃないものを弾くキーワード
    NG_NAME_RE = re.compile(
        r"(バス|バスターミナル|bus|駅前|出口|出入口|改札|駐輪|駐車|パーキング|parking|レンタカー|タクシー|交番|店|ロータリー)",
        re.IGNORECASE,
    )

    def fetch(place_type: str) -> List[Tuple[str, str, List[str]]]:
        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        loc = f"{lat},{lng}"
        data = maps_get(
            url,
            {
                "location": loc,
                "radius": str(STATION_RADIUS_M),
                "type": place_type,
                "language": "ja",
            },
        )
        out = []
        for it in (data.get("results") or []):
            name = (it.get("name") or "").strip()
            pid = (it.get("place_id") or "").strip()
            types = it.get("types") or []
            if name and pid:
                out.append((name, pid, types))
        return out

    # 優先順：train_station → transit_station → railway_station（保険）
    cands = fetch("train_station")
    if not cands:
        cands = fetch("transit_station")
    if not cands:
        cands = fetch("railway_station")

    # フィルタ：typesが駅系、かつNG語を含まない
    filtered = []
    for name, pid, types in cands:
        tset = set(types)
        if not (tset & STATION_TYPES):
            continue
        if NG_NAME_RE.search(name):
            continue
        filtered.append((name, pid))

    # 重複除去（place_id）
    seen = set()
    uniq: List[Tuple[str, str]] = []
    for n, pid in filtered:
        if pid not in seen:
            seen.add(pid)
            uniq.append((n, pid))

    return uniq[: max(1, STATION_CANDIDATES)]


def walking_minutes(origin_lat: float, origin_lng: float, dest_place_id: str) -> Optional[int]:
    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    origins = f"{origin_lat},{origin_lng}"
    destinations = f"place_id:{dest_place_id}"
    data = maps_get(
        url,
        {
            "origins": origins,
            "destinations": destinations,
            "mode": "walking",
            "language": "ja",
            "region": "jp",
        },
        timeout=60,
    )
    rows = data.get("rows") or []
    if not rows:
        return None
    elems = rows[0].get("elements") or []
    if not elems:
        return None
    el = elems[0]
    if el.get("status") != "OK":
        return None
    dur = el.get("duration", {}).get("value")  # seconds
    if dur is None:
        return None
    return int(round(float(dur) / 60.0))


def pick_best_station_by_walk(lat: float, lng: float) -> Optional[Tuple[str, int]]:
    """
    複数駅候補から徒歩分が最小の駅を選ぶ（最終「駅」判定あり）
    """
    cands = nearby_stations(lat, lng)
    if not cands:
        return None

    best_name = None
    best_min = None

    for name, pid in cands:
        wm = walking_minutes(lat, lng, pid)
        if wm is None:
            continue
        if best_min is None or wm < best_min:
            best_min = wm
            best_name = name

    if best_name is None or best_min is None:
        return None

    # 最終安全策：駅名に「駅」が無ければ採用しない（駅以外混入防止）
    if "駅" not in best_name:
        return None

    return best_name, best_min


def build_query(name: str, ward: str, address: str) -> str:
    # 住所が微妙なときほど name + 区 + 横浜市 + 保育園 で引けることが多い
    parts = [name]
    if address:
        parts.append(address)
    if ward:
        parts.append(f"横浜市{ward}")
    else:
        parts.append("横浜市")
    parts.append("保育園")
    return " ".join([p for p in parts if p]).strip()


def should_update_row(row: Dict[str, str]) -> bool:
    if FORCE_RECALC_STATION:
        return True
    if not ONLY_BAD_ROWS:
        return True

    addr = (row.get("address") or "").strip()
    lat = (row.get("lat") or "").strip()
    lng = (row.get("lng") or "").strip()
    st = (row.get("nearest_station") or "").strip()
    wm = (row.get("walk_minutes") or "").strip()

    if addr == "":
        return True
    if lat == "" or lng == "":
        return True
    if FILL_NEAREST_STATION and (st == "" or wm == ""):
        return True
    return False


def ensure_headers(rows: List[Dict[str, str]]) -> List[str]:
    base = [
        "facility_id",
        "name",
        "ward",
        "address",
        "lat",
        "lng",
        "facility_type",
        "phone",
        "website",
        "notes",
        "nearest_station",
        "walk_minutes",
        "map_url",
    ]
    existing = list(rows[0].keys()) if rows else base
    for k in base:
        if k not in existing:
            existing.append(k)
    return existing


def read_master() -> List[Dict[str, str]]:
    if not MASTER_CSV.exists():
        raise RuntimeError("data/master_facilities.csv が見つかりません")
    with MASTER_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise RuntimeError("master_facilities.csv が空です")
    return rows


def write_master(rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    with MASTER_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: (r.get(k, "") if r.get(k, "") is not None else "") for k in fieldnames})


def write_station_misses(misses: List[Dict[str, str]]) -> None:
    with STATION_MISSES_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["facility_id", "name", "ward", "reason"])
        w.writeheader()
        for r in misses:
            w.writerow(r)


def set_if(row: Dict[str, str], field: str, new_val: str, overwrite: bool) -> int:
    cur = (row.get(field) or "").strip()
    if new_val is None:
        return 0
    new_val = str(new_val).strip()
    if new_val == "":
        return 0
    if (cur == "") or overwrite:
        if cur != new_val:
            row[field] = new_val
            return 1
    return 0


def main() -> None:
    print("START fix_master_with_google_places.py")
    print(
        "WARD_FILTER=", WARD_FILTER,
        "MAX_UPDATES=", MAX_UPDATES,
        "ONLY_BAD_ROWS=", ONLY_BAD_ROWS,
        "STRICT_ADDRESS_CHECK=", STRICT_ADDRESS_CHECK
    )
    print(
        "FORCE_RECALC_STATION=", FORCE_RECALC_STATION,
        "STATION_RADIUS_M=", STATION_RADIUS_M,
        "STATION_CANDIDATES=", STATION_CANDIDATES
    )
    print("OVERWRITE_LATLNG=", OVERWRITE_LATLNG)

    rows = read_master()
    fieldnames = ensure_headers(rows)

    target = WARD_FILTER.strip() if WARD_FILTER else None

    updated_cells = 0
    updated_rows = 0
    station_misses: List[Dict[str, str]] = []

    for r in rows:
        if updated_rows >= MAX_UPDATES:
            break

        fid = (r.get("facility_id") or "").strip()
        name = (r.get("name") or "").strip()
        ward = (r.get("ward") or "").strip()

        if not fid or not name:
            continue
        if target and target not in ward:
            continue
        if not should_update_row(r):
            continue

        addr0 = (r.get("address") or "").strip()

        # 1) 園の place を引く
        query = build_query(name, ward, addr0)
        top = places_text_search(query)
        if not top:
            station_misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": "place_not_found"})
            updated_rows += 1
            continue

        place_id = (top.get("place_id") or "").strip()
        det = place_details(place_id) if place_id else None
        if not det:
            station_misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": "place_details_failed"})
            updated_rows += 1
            continue

        addr = (det.get("formatted_address") or "").strip()
        loc = (det.get("geometry") or {}).get("location") or {}
        lat = loc.get("lat")
        lng = loc.get("lng")

        phone = (det.get("formatted_phone_number") or "").strip()
        website = (det.get("website") or "").strip()
        gmap_url = (det.get("url") or "").strip()
        types = det.get("types") or []
        facility_type = (r.get("facility_type") or "").strip() or (",".join(types) if types else "")

        if addr and not ok_address(addr, ward):
            addr = ""  # 不正っぽい住所は採用しない

        # address / lat / lng
        if addr:
            updated_cells += set_if(r, "address", addr, overwrite=False)

        if (lat is not None) and (lng is not None):
            if OVERWRITE_LATLNG:
                updated_cells += set_if(r, "lat", f"{lat:.7f}", overwrite=True)
                updated_cells += set_if(r, "lng", f"{lng:.7f}", overwrite=True)
            else:
                if (r.get("lat") or "").strip() == "":
                    r["lat"] = f"{lat:.7f}"
                    updated_cells += 1
                if (r.get("lng") or "").strip() == "":
                    r["lng"] = f"{lng:.7f}"
                    updated_cells += 1

        # map_url / phone / website / type
        if gmap_url:
            updated_cells += set_if(r, "map_url", gmap_url, overwrite=OVERWRITE_MAP_URL)
        if phone:
            updated_cells += set_if(r, "phone", phone, overwrite=OVERWRITE_PHONE)
        if website:
            updated_cells += set_if(r, "website", website, overwrite=OVERWRITE_WEBSITE)
        if facility_type:
            updated_cells += set_if(r, "facility_type", facility_type, overwrite=False)

        # 2) 最寄り駅/徒歩（駅フィルタ強化＋徒歩最短）
        if FILL_NEAREST_STATION:
            lat_use = to_float(r.get("lat"))
            lng_use = to_float(r.get("lng"))

            if lat_use is None or lng_use is None:
                station_misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": "no_latlng"})
            else:
                best = pick_best_station_by_walk(lat_use, lng_use)
                if not best:
                    station_misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": "station_or_walk_failed"})
                else:
                    st_name, wm = best
                    updated_cells += set_if(r, "nearest_station", st_name, overwrite=OVERWRITE_NEAREST_STATION)
                    updated_cells += set_if(r, "walk_minutes", str(wm), overwrite=OVERWRITE_WALK_MINUTES)

        updated_rows += 1

    write_master(rows, fieldnames)
    write_station_misses(station_misses)

    print("DONE. wrote:", str(MASTER_CSV))
    print("updated rows:", updated_rows)
    print("updated cells:", updated_cells)
    print("station misses:", len(station_misses), "->", str(STATION_MISSES_CSV))


if __name__ == "__main__":
    main()
