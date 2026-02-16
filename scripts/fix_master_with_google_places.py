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

STATION_CACHE = DATA_DIR / "stations_cache_yokohama.json"
STATION_MISSES = DATA_DIR / "station_misses.csv"

ALLOWED_STATION_TYPES = {
    "train_station",
    "subway_station",
    "transit_station",
    "light_rail_station",
}

# 強制除外ワード（駅以外の混入を抑える）
BAD_STATION_WORDS = [
    "バス", "バス停", "交差点", "公園", "小学校", "中学校", "高校", "病院", "クリニック",
    "消防", "警察", "区役所", "市役所", "郵便局", "図書館", "体育館", "保育園", "幼稚園",
    "こども園", "店", "スーパー", "コンビニ", "薬局", "営業所", "本社", "支店", "工場",
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

def looks_like_station_name(name: str) -> bool:
    n = safe(name).strip()
    if not n:
        return False
    # 明確に駅でない語を含む
    for w in BAD_STATION_WORDS:
        if w in n:
            return False
    # 「駅」を含む or 末尾「…駅」
    if n.endswith("駅") or ("駅" in n):
        return True
    # 例外：地名だけ返ることがあるので「新横浜」「日吉」等は許容し、後で「駅」付ける
    # ただし漢字2〜6文字程度の短い地名っぽいものだけ
    if re.fullmatch(r"[一-龥ぁ-んァ-ヶー]{2,8}", n):
        return True
    return False

def normalize_station_name(name: str) -> str:
    n = safe(name).strip()
    # すでに「駅」が付いていればそのまま
    if n.endswith("駅"):
        return n
    # 「〇〇駅」の中に含まれてる場合は切り出し
    m = re.search(r"(.+?駅)", n)
    if m:
        return m.group(1)
    # 地名だけなら「駅」付け
    if looks_like_station_name(n):
        return n + "駅"
    return n

def is_station_candidate(place: Dict[str, Any]) -> bool:
    name = safe(place.get("name")).strip()
    types = set(place.get("types") or [])
    # types が駅系を含むこと（最重要）
    if not (types & ALLOWED_STATION_TYPES):
        return False
    # 名前が駅っぽいこと
    if not looks_like_station_name(name):
        return False
    return True

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

def nearby_stations(lat: float, lng: float, radius_m: int) -> List[Dict[str, Any]]:
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    # type は一つしか指定できないので transit_station を使う（subway/trainを types で後段判定）
    js = g_get(url, {
        "location": f"{lat},{lng}",
        "radius": radius_m,
        "type": "transit_station",
        "key": API_KEY,
        "language": "ja",
    })
    if js.get("status") not in ("OK", "ZERO_RESULTS"):
        return []
    return js.get("results") or []

def text_search_station(lat: float, lng: float, radius_m: int, hint: str) -> List[Dict[str, Any]]:
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    # locationbias を半径で与える（近傍優先）
    q = f"{hint} 駅"
    js = g_get(url, {
        "query": q,
        "location": f"{lat},{lng}",
        "radius": radius_m,
        "key": API_KEY,
        "language": "ja",
        "region": "jp",
    })
    if js.get("status") not in ("OK", "ZERO_RESULTS"):
        return []
    return js.get("results") or []

# ---------------- station cache (optional) ----------------
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
    pid = safe(place.get("place_id"))
    if not pid:
        return
    items = cache.setdefault("stations", [])
    if any(s.get("place_id") == pid for s in items):
        return
    name = safe(place.get("name"))
    loc = (place.get("geometry") or {}).get("location") or {}
    items.append({
        "place_id": pid,
        "name": normalize_station_name(name),
        "lat": loc.get("lat"),
        "lng": loc.get("lng"),
        "types": place.get("types") or [],
    })

def choose_best_station(lat: float, lng: float, candidates: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    # 駅フィルタ
    good = [p for p in candidates if is_station_candidate(p)]
    if not good:
        return None
    # 近い順
    def dist(p):
        loc = (p.get("geometry") or {}).get("location") or {}
        try:
            return haversine_m(lat, lng, float(loc.get("lat")), float(loc.get("lng")))
        except Exception:
            return 1e18
    good.sort(key=dist)
    return good[0]

def nearest_station_for(lat: float, lng: float, hint_name: str, radius_m: int, cache: Dict[str, Any]) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """
    returns (station_name, walk_minutes, station_place_id)
    walk_minutes は直線距離からの簡易換算（徒歩80m/分）で十分（本来はDirectionsだがコスト増）
    """
    # 1) nearby
    cands = nearby_stations(lat, lng, radius_m)
    best = choose_best_station(lat, lng, cands)

    # 2) fallback: text search
    if best is None:
        cands2 = text_search_station(lat, lng, radius_m, hint_name)
        best = choose_best_station(lat, lng, cands2)

    if best is None:
        return None, None, None

    upsert_station_cache(cache, best)

    name = normalize_station_name(safe(best.get("name")))
    pid = safe(best.get("place_id")) or None

    # walk minutes: 80m/min 想定（ざっくり）
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
    # fields の不足は追加
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

def bad_station_value(st: str) -> bool:
    s = safe(st).strip()
    if s == "" or s.lower() == "null" or s == "-":
        return True
    # “駅” がない & かつ地名っぽくないものは怪しい
    if (not s.endswith("駅")) and (not re.fullmatch(r"[一-龥ぁ-んァ-ヶー]{2,8}", s)):
        return True
    for w in BAD_STATION_WORDS:
        if w in s:
            return True
    return False

def main() -> None:
    rows, fields = read_master_rows()

    # フィルタ
    target_ward = WARD_FILTER.strip() if WARD_FILTER else None

    # station cache
    cache = load_station_cache()

    misses: List[Dict[str, Any]] = []
    updated_cells = 0
    updated_rows = 0

    for row in rows:
        fid = safe(row.get("facility_id")).strip()
        name = norm_spaces(row.get("name",""))
        ward = safe(row.get("ward")).strip()

        if target_ward and target_ward not in ward:
            continue

        # 更新対象判定
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
            # 空欄があるなら対象
            if (not addr0) or (not lat0) or (not lng0) or (FILL_NEAREST_STATION and (not st0 or bad_station_value(st0))):
                needs = True

        if not needs:
            continue

        if updated_rows >= MAX_UPDATES:
            break

        # --- geocode: place details を取りたいので query を作る
        q = " ".join([name, ward, CITY_FILTER, "日本"]).strip()
        geo = geocode_place(q)
        if not geo:
            misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": "geocode_failed", "query_tried": q})
            continue

        place_id = safe(geo.get("place_id"))
        det = place_details(place_id) if place_id else None
        if not det:
            # detailsが無い場合も最低限geocodeから拾う
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

        # update address/lat/lng/map_url etc.
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
        c += set_if("address", formatted_address, True)  # 住所は常に置き換えたい方が多い
        c += set_if("lat", lat, True)
        c += set_if("lng", lng, True)
        c += set_if("facility_type", ",".join(det.get("types") or []), True)
        c += set_if("phone", det.get("international_phone_number"), OVERWRITE_PHONE)
        c += set_if("website", det.get("website"), OVERWRITE_WEBSITE)
        c += set_if("map_url", det.get("url"), OVERWRITE_MAP_URL)

        # nearest station
        if FILL_NEAREST_STATION and lat and lng:
            try:
                st_name, walk_min, st_pid = nearest_station_for(float(lat), float(lng), name, NEARBY_RADIUS_M, cache)
                if st_name:
                    if OVERWRITE_NEAREST_STATION or bad_station_value(st0) or st0 == "":
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

    # save cache
    save_station_cache(cache)

    # write misses
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
