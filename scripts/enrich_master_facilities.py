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
OUT_CSV = DATA_DIR / "master_facilities.csv"  # 上書き
CACHE_JSON = DATA_DIR / "geocode_cache.json"

WARD_HINT = os.getenv("WARD_FILTER", "港北区").strip() or "港北区"
GOOGLE_API_KEY = (os.getenv("GOOGLE_API_KEY") or "").strip()

# 徒歩分の換算（m/分）
WALK_SPEED_M_PER_MIN = float(os.getenv("WALK_SPEED_M_PER_MIN", "80"))

# ---- 港北区周辺 主要駅（必要なら追加/修正） ----
# ざっくりでも「最寄り駅」の初期値として十分効く
STATIONS: List[Dict[str, Any]] = [
    {"name": "日吉駅", "lat": 35.5533, "lng": 139.6467},
    {"name": "綱島駅", "lat": 35.5366, "lng": 139.6340},
    {"name": "大倉山駅", "lat": 35.5228, "lng": 139.6296},
    {"name": "菊名駅", "lat": 35.5096, "lng": 139.6305},
    {"name": "新横浜駅", "lat": 35.5069, "lng": 139.6170},
    {"name": "妙蓮寺駅", "lat": 35.4978, "lng": 139.6346},
    {"name": "白楽駅", "lat": 35.4868, "lng": 139.6250},
    {"name": "小机駅", "lat": 35.5153, "lng": 139.5978},
    {"name": "新羽駅", "lat": 35.5270, "lng": 139.6119},
    {"name": "北新横浜駅", "lat": 35.5186, "lng": 139.6091},
    {"name": "高田駅", "lat": 35.5484, "lng": 139.6146},
    {"name": "日吉本町駅", "lat": 35.5557, "lng": 139.6318},
    {"name": "大倉山駅", "lat": 35.5228, "lng": 139.6296},
    {"name": "岸根公園駅", "lat": 35.4937, "lng": 139.6123},
]

# ---------------- utils ----------------
def norm(s: Any) -> str:
    if s is None:
        return ""
    x = str(s).replace("　", " ").strip()
    x = re.sub(r"\s+", " ", x)
    return x.strip()

def is_blank(s: Any) -> bool:
    return norm(s) == ""

def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dphi/2)**2 + math.cos(p1) * math.cos(p2) * math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def load_cache() -> Dict[str, Any]:
    if CACHE_JSON.exists():
        try:
            return json.loads(CACHE_JSON.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_cache(cache: Dict[str, Any]) -> None:
    CACHE_JSON.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def guess_nearest_station(lat: float, lng: float) -> Tuple[str, int]:
    best_name = ""
    best_m = 10**18
    for st in STATIONS:
        d = haversine_m(lat, lng, float(st["lat"]), float(st["lng"]))
        if d < best_m:
            best_m = d
            best_name = st["name"]
    walk_min = int(math.ceil(best_m / WALK_SPEED_M_PER_MIN))
    return best_name, walk_min

# ---------------- geocoding ----------------
def google_places_lookup(name: str, ward: str, address_hint: str = "") -> Optional[Dict[str, Any]]:
    """
    Google Places Text Search -> Place Details（住所/電話/website/latlng）
    """
    if not GOOGLE_API_KEY:
        return None

    q = " ".join([name, address_hint, f"横浜市{ward}"]).strip()
    q = re.sub(r"\s+", " ", q)

    # Text Search
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    r = requests.get(url, params={"query": q, "key": GOOGLE_API_KEY, "language": "ja"}, timeout=30)
    r.raise_for_status()
    js = r.json()
    results = js.get("results", [])
    if not results:
        return None

    place_id = results[0].get("place_id")
    if not place_id:
        return None

    # Details
    url2 = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = "name,formatted_address,geometry,international_phone_number,formatted_phone_number,website,url"
    r2 = requests.get(url2, params={"place_id": place_id, "fields": fields, "key": GOOGLE_API_KEY, "language": "ja"}, timeout=30)
    r2.raise_for_status()
    d = r2.json().get("result", {})

    loc = (d.get("geometry") or {}).get("location") or {}
    return {
        "address": d.get("formatted_address", ""),
        "lat": loc.get("lat"),
        "lng": loc.get("lng"),
        "phone": d.get("formatted_phone_number") or d.get("international_phone_number") or "",
        "website": d.get("website") or "",
        "map_url": d.get("url") or "",
    }

def nominatim_lookup(name: str, ward: str, address_hint: str = "") -> Optional[Dict[str, Any]]:
    """
    OpenStreetMap Nominatim（無料）: 住所/latlng
    ※レート制限があるのでキャッシュ必須。1秒スリープも入れる。
    """
    q = " ".join([name, address_hint, f"横浜市{ward}", "日本"]).strip()
    q = re.sub(r"\s+", " ", q)

    url = "https://nominatim.openstreetmap.org/search"
    headers = {"User-Agent": "NurseryAvailabilityBot/1.0 (non-commercial; contact: github)"}
    r = requests.get(url, params={"q": q, "format": "json", "limit": 1, "addressdetails": 1}, headers=headers, timeout=30)
    r.raise_for_status()
    arr = r.json()
    if not arr:
        return None
    hit = arr[0]
    lat = float(hit["lat"])
    lng = float(hit["lon"])
    disp = hit.get("display_name") or ""
    # display_name は長いので、最低限だけ残す（必要なら加工してOK）
    return {"address": disp, "lat": lat, "lng": lng, "phone": "", "website": "", "map_url": ""}

def lookup_any(name: str, ward: str, address_hint: str, cache: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    key = f"{ward}::{name}"
    if key in cache:
        return cache[key]

    out = google_places_lookup(name, ward, address_hint)
    if out is None:
        out = nominatim_lookup(name, ward, address_hint)

    if out is not None:
        cache[key] = out
        save_cache(cache)
    return out

# ---------------- main ----------------
def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def write_csv(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

def main() -> None:
    if not MASTER_CSV.exists():
        raise FileNotFoundError(f"not found: {MASTER_CSV}")

    rows = read_csv(MASTER_CSV)
    if not rows:
        raise RuntimeError("master_facilities.csv is empty")

    # 既存ヘッダ＋必要列が無ければ追加
    fieldnames = list(rows[0].keys())
    needed = ["nearest_station", "walk_minutes", "phone", "website", "facility_type", "map_url", "lat", "lng", "address", "ward", "name", "facility_id"]
    for k in needed:
        if k not in fieldnames:
            fieldnames.append(k)

    cache = load_cache()

    updated = 0
    for i, r in enumerate(rows, 1):
        name = norm(r.get("name"))
        if not name:
            continue

        ward = norm(r.get("ward")) or WARD_HINT
        address_hint = norm(r.get("address"))

        # 取得したいもの：住所 or latlng が無い場合にlookup
        need_lookup = (
            is_blank(r.get("address")) or
            is_blank(r.get("lat")) or is_blank(r.get("lng")) or
            is_blank(r.get("map_url")) or
            is_blank(r.get("phone")) or
            is_blank(r.get("website"))
        )

        if need_lookup:
            out = lookup_any(name, ward, address_hint, cache)
            if out:
                # address
                if is_blank(r.get("address")) and out.get("address"):
                    r["address"] = str(out["address"])
                # lat/lng
                if (is_blank(r.get("lat")) or is_blank(r.get("lng"))) and out.get("lat") is not None and out.get("lng") is not None:
                    r["lat"] = str(out["lat"])
                    r["lng"] = str(out["lng"])
                # phone/website/map_url
                if is_blank(r.get("phone")) and out.get("phone"):
                    r["phone"] = str(out["phone"])
                if is_blank(r.get("website")) and out.get("website"):
                    r["website"] = str(out["website"])
                if is_blank(r.get("map_url")) and out.get("map_url"):
                    r["map_url"] = str(out["map_url"])

                updated += 1

            # 無料フォールバック時は連続アクセス抑制
            if not GOOGLE_API_KEY:
                time.sleep(1.0)

        # 最寄り駅・徒歩分（latlngが揃ったら推定）
        try:
            lat = float(r.get("lat") or 0)
            lng = float(r.get("lng") or 0)
            if lat != 0 and lng != 0:
                if is_blank(r.get("nearest_station")) or is_blank(r.get("walk_minutes")):
                    st, wm = guess_nearest_station(lat, lng)
                    if is_blank(r.get("nearest_station")):
                        r["nearest_station"] = st
                    if is_blank(r.get("walk_minutes")):
                        r["walk_minutes"] = str(wm)
        except Exception:
            pass

        if i % 50 == 0:
            print(f"processed {i}/{len(rows)} ... updated={updated}")

    write_csv(OUT_CSV, rows, fieldnames)
    print("DONE. wrote:", OUT_CSV)
    print("updated rows:", updated)

if __name__ == "__main__":
    main()
