#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
import math
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

API_KEY = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
if not API_KEY:
    raise RuntimeError("GOOGLE_MAPS_API_KEY が未設定です（Secretsに設定してください）")

CITY_FILTER = (os.getenv("CITY_FILTER", "横浜市") or "").strip()
WARD_FILTER = (os.getenv("WARD_FILTER", "") or "").strip() or None

MAX_UPDATES = int(os.getenv("MAX_UPDATES", "200"))
ONLY_BAD_ROWS = (os.getenv("ONLY_BAD_ROWS", "0") == "1")
STRICT_ADDRESS_CHECK = (os.getenv("STRICT_ADDRESS_CHECK", "1") == "1")
SLEEP_SEC = float(os.getenv("GOOGLE_API_SLEEP_SEC", "0.15"))

OVERWRITE_PHONE = (os.getenv("OVERWRITE_PHONE", "0") == "1")
OVERWRITE_WEBSITE = (os.getenv("OVERWRITE_WEBSITE", "0") == "1")
OVERWRITE_MAP_URL = (os.getenv("OVERWRITE_MAP_URL", "0") == "1")
OVERWRITE_NEAREST_STATION = (os.getenv("OVERWRITE_NEAREST_STATION", "1") == "1")
OVERWRITE_WALK_MINUTES = (os.getenv("OVERWRITE_WALK_MINUTES", "1") == "1")
FILL_NEAREST_STATION = (os.getenv("FILL_NEAREST_STATION", "1") == "1")

NEARBY_RADIUS_M = int(os.getenv("NEARBY_RADIUS_M", "2500"))
FORCE_REBUILD_STATIONS = (os.getenv("FORCE_REBUILD_STATIONS", "0") == "1")

# 既存の駅名が駅じゃなければ空に戻して再計算（精度優先）
SANITIZE_BAD_EXISTING_STATION = (os.getenv("SANITIZE_BAD_EXISTING_STATION", "1") == "1")

STATION_CACHE = DATA_DIR / "stations_cache_yokohama.json"
STATION_MISSES = DATA_DIR / "station_misses.csv"

# ★駅として許可する types（駅専用のみ）
ALLOWED_STATION_TYPES = {
    "train_station",
    "subway_station",
    "light_rail_station",
}

# ★明確に弾く types（バス停・バスターミナル等）
DISALLOWED_STATION_TYPES = {
    "bus_station",
    "bus_stop",
    "bus",
}

# 強制除外ワード（駅以外の混入を抑える）
BAD_STATION_WORDS = [
    "バス", "バス停", "交差点", "公園", "小学校", "中学校", "高校", "病院", "クリニック",
    "消防", "警察", "区役所", "市役所", "郵便局", "図書館", "体育館", "保育園", "幼稚園",
    "こども園", "店", "スーパー", "コンビニ", "薬局", "営業所", "本社", "支店", "工場",
    "交番", "入口", "寺", "神社", "橋", "踏切",
    "二丁目", "三丁目", "四丁目", "五丁目", "丁目",
    "番地", "番", "号",
    "プラウド", "シティ", "レジデンス", "マンション", "団地", "ハイツ", "コーポ",
    "SST", "前", "通り", "新道",
]


# ---------------- small utils ----------------
def safe(x: Any) -> str:
    return "" if x is None else str(x)

def norm_spaces(s: str) -> str:
    s = safe(s).replace("　", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def in_scope_address(addr: str, city: str, ward: Optional[str]) -> bool:
    a = safe(addr)
    if not a:
        return False
    if STRICT_ADDRESS_CHECK:
        if city and city not in a:
            return False
        if ward and ward not in a:
            return False
    return True

def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlng/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def normalize_station_name(name: str) -> str:
    n = safe(name).strip()
    if not n:
        return ""
    if n.endswith("駅"):
        return n
    m = re.search(r"(.+?駅)", n)
    if m:
        return m.group(1).strip()
    return n

def looks_like_station_name(name: str) -> bool:
    """
    精度優先：駅名は “〇〇駅” 形式のみ採用
    （駅前/駅通り/駅入口 etc. は弾く）
    """
    n = normalize_station_name(name)
    if not n:
        return False

    # “〇〇駅” で終わることを必須（これで「〇〇駅前」を排除）
    if not n.endswith("駅"):
        return False

    # 住所っぽいもの排除
    if re.search(r"\d+丁目", n) or re.search(r"\d+番", n) or re.search(r"\d+号", n):
        return False
    if "丁目" in n or "番地" in n:
        return False

    # 駅じゃない語を含むのは排除
    for w in BAD_STATION_WORDS:
        if w in n:
            return False

    return True

def bad_station_value(st: str) -> bool:
    s = safe(st).strip()
    if s == "" or s.lower() == "null" or s == "-":
        return True
    s2 = normalize_station_name(s)
    if not s2.endswith("駅"):
        return True
    for w in BAD_STATION_WORDS:
        if w in s2:
            return True
    return False

def sanitize_existing_station_fields(row: Dict[str, str]) -> int:
    if not SANITIZE_BAD_EXISTING_STATION:
        return 0
    st = safe(row.get("nearest_station")).strip()
    wk = safe(row.get("walk_minutes")).strip()
    changed = 0
    if st and bad_station_value(st):
        row["nearest_station"] = ""
        row["station_kana"] = ""
        changed += 1
    if (not safe(row.get("nearest_station")).strip()) and wk:
        row["walk_minutes"] = ""
        changed += 1
    return changed


# ---------------- Google APIs ----------------
def g_get(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    time.sleep(SLEEP_SEC)
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def geocode_place(query: str) -> Optional[Dict[str, Any]]:
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    js = g_get(url, {"address": query, "key": API_KEY, "language": "ja", "region": "jp"})
    if js.get("status") != "OK":
        return None
    return js["results"][0]

def place_details(place_id: str) -> Optional[Dict[str, Any]]:
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = "name,formatted_address,geometry/location,types,international_phone_number,website,url"
    js = g_get(url, {"place_id": place_id, "fields": fields, "key": API_KEY, "language": "ja"})
    if js.get("status") != "OK":
        return None
    return js.get("result") or None

def nearby_search(lat: float, lng: float, radius_m: int, place_type: str) -> List[Dict[str, Any]]:
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    js = g_get(url, {
        "location": f"{lat},{lng}",
        "radius": radius_m,
        "type": place_type,
        "keyword": "駅",  # ★駅に寄せる
        "key": API_KEY,
        "language": "ja",
    })
    if js.get("status") not in ("OK", "ZERO_RESULTS"):
        return []
    return js.get("results") or []

def text_search_station_near(lat: float, lng: float, radius_m: int) -> List[Dict[str, Any]]:
    """
    fallback：近傍の “駅” を拾う。ここは混ざるので details で再検証する。
    """
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    js = g_get(url, {
        "query": "駅",
        "location": f"{lat},{lng}",
        "radius": radius_m,
        "key": API_KEY,
        "language": "ja",
        "region": "jp",
    })
    if js.get("status") not in ("OK", "ZERO_RESULTS"):
        return []
    return js.get("results") or []


# ---------------- station validation ----------------
def is_station_by_types(types: List[str]) -> bool:
    tset = set(types or [])
    # ★バス要素が入ってたら即 reject
    if tset & DISALLOWED_STATION_TYPES:
        return False
    # ★駅専用 type が1つでも必要
    return bool(tset & ALLOWED_STATION_TYPES)

def is_station_candidate_shallow(place: Dict[str, Any]) -> bool:
    """
    検索結果だけで軽く判定（最終確定は details で行う）
    """
    name = normalize_station_name(safe(place.get("name")))
    if not looks_like_station_name(name):
        return False

    types = place.get("types") or []
    # 検索結果の types は揺れるので、ここでは “バスっぽい” のみ落とし、駅typeは必須にしない
    # → ただし bus_station/bus_stop は確実に落とす
    if set(types) & DISALLOWED_STATION_TYPES:
        return False

    return True

def validate_station_with_details(place: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    ★最重要：place details の types で「駅だけ」に確定する
    """
    pid = safe(place.get("place_id")).strip()
    if not pid:
        return None
    det = place_details(pid)
    if not det:
        return None

    name = normalize_station_name(safe(det.get("name")))
    if not looks_like_station_name(name):
        return None

    types = det.get("types") or []
    if not is_station_by_types(types):
        return None

    # details の location を持つ shape に合わせて返す
    out = dict(place)
    out["name"] = name
    out["types"] = types
    # geometry は details から取る（なければ元のまま）
    if det.get("geometry"):
        out["geometry"] = det["geometry"]
    return out


# ---------------- station cache ----------------
def load_station_cache() -> Dict[str, Any]:
    if FORCE_REBUILD_STATIONS and STATION_CACHE.exists():
        STATION_CACHE.unlink()
    if not STATION_CACHE.exists():
        return {"stations": []}
    try:
        return json.loads(STATION_CACHE.read_text(encoding="utf-8"))
    except Exception:
        return {"stations": []}

def save_station_cache(obj: Dict[str, Any]) -> None:
    STATION_CACHE.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def upsert_station_cache(cache: Dict[str, Any], place: Dict[str, Any]) -> None:
    pid = safe(place.get("place_id")).strip()
    if not pid:
        return
    items = cache.setdefault("stations", [])
    if any(s.get("place_id") == pid for s in items):
        return
    loc = (place.get("geometry") or {}).get("location") or {}
    items.append({
        "place_id": pid,
        "name": normalize_station_name(safe(place.get("name"))),
        "lat": loc.get("lat"),
        "lng": loc.get("lng"),
        "types": place.get("types") or [],
    })

def nearest_station_for(lat: float, lng: float, radius_m: int, cache: Dict[str, Any]) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """
    returns (station_name, walk_minutes, station_place_id)
    徒歩は直線距離→80m/分換算（Directionsを使わずコスト抑制）
    """

    # 1) 駅専用タイプで nearbysearch（ここでバス停は基本入らない）
    raw: List[Dict[str, Any]] = []
    for t in ["train_station", "subway_station", "light_rail_station"]:
        raw.extend(nearby_search(lat, lng, radius_m, place_type=t))

    # 2) fallback: text search（混ざるので details で確定）
    if not raw:
        raw = text_search_station_near(lat, lng, radius_m)

    # 3) 距離順に並べる（shallowで駅名っぽいものだけ残す）
    cands = [p for p in raw if is_station_candidate_shallow(p)]
    if not cands:
        return None, None, None

    def dist(p):
        loc = (p.get("geometry") or {}).get("location") or {}
        try:
            return haversine_m(lat, lng, float(loc.get("lat")), float(loc.get("lng")))
        except Exception:
            return 1e18

    cands.sort(key=dist)

    # 4) 近い順に最大5件まで details で「駅だけ」確定（ここでバス停を完全排除）
    best: Optional[Dict[str, Any]] = None
    for p in cands[:5]:
        v = validate_station_with_details(p)
        if v:
            best = v
            break

    if best is None:
        return None, None, None

    name = normalize_station_name(safe(best.get("name")))
    pid = safe(best.get("place_id")) or None

    upsert_station_cache(cache, best)

    loc = (best.get("geometry") or {}).get("location") or {}
    try:
        d = haversine_m(lat, lng, float(loc.get("lat")), float(loc.get("lng")))
        walk = int(round(d / 80.0))
        walk = max(1, walk)
    except Exception:
        walk = None

    return name, walk, pid


# ---------------- master I/O ----------------
def read_master_rows() -> Tuple[List[Dict[str, str]], List[str]]:
    if not MASTER_CSV.exists():
        raise RuntimeError("data/master_facilities.csv がありません")
    with MASTER_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        rows = list(r)
        fields = r.fieldnames or []
    return rows, fields

def write_master_rows(rows: List[Dict[str, str]], fields: List[str]) -> None:
    want_cols = [
        "facility_id","name","ward","address","lat","lng","map_url",
        "facility_type","phone","website","notes",
        "nearest_station","walk_minutes",
        "name_kana","station_kana",
    ]
    for c in want_cols:
        if c not in fields:
            fields.append(c)

    with MASTER_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fields})


def main() -> None:
    rows, fields = read_master_rows()
    target_ward = WARD_FILTER.strip() if WARD_FILTER else None

    cache = load_station_cache()

    misses: List[Dict[str, Any]] = []
    updated_cells = 0
    updated_rows = 0

    for row in rows:
        fid = safe(row.get("facility_id")).strip()
        name = norm_spaces(row.get("name", ""))
        ward = safe(row.get("ward")).strip()

        if target_ward and target_ward not in ward:
            continue

        # 既存の駅が駅じゃないなら空に戻してやり直す
        updated_cells += sanitize_existing_station_fields(row)

        addr0 = safe(row.get("address")).strip()
        lat0 = safe(row.get("lat")).strip()
        lng0 = safe(row.get("lng")).strip()
        st0  = safe(row.get("nearest_station")).strip()
        wk0  = safe(row.get("walk_minutes")).strip()

        needs = False
        if ONLY_BAD_ROWS:
            if (not in_scope_address(addr0, CITY_FILTER, target_ward)) or bad_station_value(st0) or wk0 in ("", "null", "-"):
                needs = True
        else:
            if (not addr0) or (not lat0) or (not lng0) or (FILL_NEAREST_STATION and (not st0 or bad_station_value(st0))):
                needs = True

        if not needs:
            continue

        if updated_rows >= MAX_UPDATES:
            break

        # geocode query（“保育園”を入れると精度が上がりやすい）
        q_parts = [name]
        if ward:
            q_parts.append(ward)
        if CITY_FILTER:
            q_parts.append(CITY_FILTER)
        q_parts.append("保育園")
        q_parts.append("日本")
        q = " ".join([p for p in q_parts if p]).strip()

        geo = geocode_place(q)
        if not geo:
            misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": "geocode_failed", "query_tried": q})
            continue

        place_id = safe(geo.get("place_id"))
        det = place_details(place_id) if place_id else None
        if not det:
            det = {
                "name": name,
                "formatted_address": (geo.get("formatted_address") if geo else ""),
                "geometry": geo.get("geometry"),
                "types": geo.get("types") or [],
                "url": "",
                "website": "",
                "international_phone_number": "",
            }

        formatted_address = safe(det.get("formatted_address")).strip()
        loc = ((det.get("geometry") or {}).get("location") or {})
        lat = safe(loc.get("lat")).strip()
        lng = safe(loc.get("lng")).strip()

        if STRICT_ADDRESS_CHECK and not in_scope_address(formatted_address, CITY_FILTER, target_ward):
            misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": "address_out_of_scope", "query_tried": q})
            continue

        def set_if(col: str, val: Any, overwrite: bool) -> int:
            v = safe(val).strip()
            if v == "":
                return 0
            cur = safe(row.get(col)).strip()
            if overwrite or cur == "":
                if cur != v:
                    row[col] = v
                    return 1
            return 0

        c = 0
        c += set_if("address", formatted_address, True)
        c += set_if("lat", lat, True)
        c += set_if("lng", lng, True)
        c += set_if("facility_type", ",".join(det.get("types") or []), True)
        c += set_if("phone", det.get("international_phone_number"), OVERWRITE_PHONE)
        c += set_if("website", det.get("website"), OVERWRITE_WEBSITE)
        c += set_if("map_url", det.get("url"), OVERWRITE_MAP_URL)

        # nearest station（detailsで駅確定→バス停排除）
        if FILL_NEAREST_STATION and lat and lng:
            try:
                st_name, walk_min, _st_pid = nearest_station_for(float(lat), float(lng), NEARBY_RADIUS_M, cache)

                if st_name:
                    if OVERWRITE_NEAREST_STATION or st0 == "" or bad_station_value(st0):
                        if safe(row.get("nearest_station")).strip() != st_name:
                            row["nearest_station"] = st_name
                            c += 1

                if walk_min is not None:
                    if OVERWRITE_WALK_MINUTES or wk0 in ("", "null", "-"):
                        if safe(row.get("walk_minutes")).strip() != str(walk_min):
                            row["walk_minutes"] = str(walk_min)
                            c += 1

            except Exception as e:
                misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": f"station_failed:{e}", "query_tried": q})

        if c > 0:
            updated_cells += c
            updated_rows += 1

    save_station_cache(cache)

    if misses:
        write_csv(
            STATION_MISSES,
            misses,
            fieldnames=["facility_id","name","ward","reason","query_tried"],
        )

    write_master_rows(rows, fields)

    print("DONE. wrote:", str(MASTER_CSV))
    print("updated rows:", updated_rows, "updated cells:", updated_cells)
    print("station cache:", str(STATION_CACHE), "count:", len((cache.get("stations") or [])))
    print("misses:", len(misses), f"(see {STATION_MISSES.name})" if misses else "")

if __name__ == "__main__":
    main()
