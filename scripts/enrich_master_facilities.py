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
OUT_CSV = DATA_DIR / "master_facilities.csv"          # 上書き
CACHE_JSON = DATA_DIR / "geocode_cache.json"
MISSES_CSV = DATA_DIR / "geocode_misses.csv"

WARD_HINT = (os.getenv("WARD_FILTER", "港北区") or "").strip() or "港北区"

WALK_SPEED_M_PER_MIN = float(os.getenv("WALK_SPEED_M_PER_MIN", "80"))
SLEEP_SEC = float(os.getenv("NOMINATIM_SLEEP_SEC", "1.1"))
RETRY = int(os.getenv("NOMINATIM_RETRY", "4"))
MAX_CANDIDATES = int(os.getenv("NOMINATIM_MAX_CANDIDATES", "5"))

# ---- 港北区周辺 主要駅（必要なら追加） ----
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

def normalize_name(name: str) -> str:
    """
    ヒット率向上用の正規化：
    - 全角スペース→半角
    - 連続スペース削減
    - カッコ内注記を除去（例：〇〇保育園（分園））
    - 記号ゆれを軽減
    """
    x = norm(name)
    x = re.sub(r"[（\(].*?[）\)]", "", x).strip()
    x = x.replace("・", "").replace("　", " ")
    x = re.sub(r"\s+", " ", x).strip()
    return x

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

def build_map_url(lat: float, lng: float) -> str:
    return f"https://www.google.com/maps/search/?api=1&query={lat},{lng}"

def score_candidate(hit: Dict[str, Any], ward: str) -> int:
    """
    Nominatim候補を “横浜市 + 港北区” に寄せて選ぶスコア。
    """
    s = 0
    disp = (hit.get("display_name") or "")
    addr = (hit.get("address") or {})

    if "横浜市" in disp:
        s += 50
    if ward and ward in disp:
        s += 40

    # addressdetails からも加点
    city = str(addr.get("city") or addr.get("town") or addr.get("municipality") or "")
    county = str(addr.get("county") or "")
    suburb = str(addr.get("suburb") or addr.get("city_district") or "")

    if "横浜" in city:
        s += 30
    if ward and (ward in county or ward in suburb):
        s += 30

    # 日本であること（country）
    if str(addr.get("country") or "") in ("日本", "Japan"):
        s += 10

    return s

# ---------------- nominatim ----------------
def nominatim_search(q: str) -> List[Dict[str, Any]]:
    url = "https://nominatim.openstreetmap.org/search"
    headers = {
        "User-Agent": "NurseryAvailabilityBot/1.0 (non-commercial; github actions)",
        "Accept-Language": "ja",
    }
    params = {
        "q": q,
        "format": "json",
        "limit": MAX_CANDIDATES,
        "addressdetails": 1,
    }

    for t in range(RETRY):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=40)
            if r.status_code == 429:
                # レート制限：指数バックオフ
                time.sleep(max(SLEEP_SEC, 2.0) * (t + 1))
                continue
            r.raise_for_status()
            arr = r.json()
            return arr if isinstance(arr, list) else []
        except Exception as e:
            print("WARN nominatim error:", e)
            time.sleep(SLEEP_SEC * (t + 1))
    return []

def lookup_nominatim(facility_id: str, name: str, ward: str, address_hint: str, cache: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    戻り値: (out or None, used_query)
    キャッシュキーは facility_id 優先（園名ゆれに強い）
    """
    fid = norm(facility_id)
    key = f"fid::{fid}" if fid else f"{ward}::{name}"
    if key in cache:
        return cache[key], cache[key].get("q", "")

    nm = normalize_name(name)
    queries: List[str] = []

    # 住所ヒントがある場合は最優先
    if address_hint:
        queries.append(f"{nm} {address_hint} 横浜市{ward} 日本")

    # 基本クエリ（園名 + 区 + 横浜市）
    queries.append(f"{nm} 横浜市{ward} 日本")

    # “保育園/保育所/こども園” のゆれを保険で追加
    if "保育" not in nm:
        queries.append(f"{nm} 保育園 横浜市{ward} 日本")
        queries.append(f"{nm} 保育所 横浜市{ward} 日本")
    queries.append(f"{nm} 認定こども園 横浜市{ward} 日本")

    used_q = ""
    for q in queries:
        q = re.sub(r"\s+", " ", q).strip()
        used_q = q
        hits = nominatim_search(q)
        time.sleep(SLEEP_SEC)

        if not hits:
            continue

        # スコア最大の候補を選ぶ
        best = max(hits, key=lambda h: score_candidate(h, ward))

        lat = float(best["lat"])
        lng = float(best["lon"])
        disp = best.get("display_name") or ""

        out = {
            "address": disp,
            "lat": lat,
            "lng": lng,
            "map_url": build_map_url(lat, lng),
            "q": used_q,
        }
        cache[key] = out
        save_cache(cache)
        return out, used_q

    return None, used_q

# ---------------- csv i/o ----------------
def read_csv_file(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def write_csv_file(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

def ensure_columns(fieldnames: List[str]) -> List[str]:
    needed = [
        "facility_id","name","ward","address","lat","lng",
        "facility_type","phone","website","notes",
        "nearest_station","walk_minutes","map_url"
    ]
    for k in needed:
        if k not in fieldnames:
            fieldnames.append(k)
    return fieldnames

def write_misses(misses: List[Dict[str, str]]) -> None:
    if not misses:
        # 以前のファイルが残ると紛らわしいので空でも書き換える
        MISSES_CSV.write_text("facility_id,name,ward,query_tried\n", encoding="utf-8")
        return
    with MISSES_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["facility_id","name","ward","query_tried"])
        w.writeheader()
        for r in misses:
            w.writerow(r)

def main() -> None:
    if not MASTER_CSV.exists():
        raise FileNotFoundError(f"not found: {MASTER_CSV}")

    rows = read_csv_file(MASTER_CSV)
    if not rows:
        raise RuntimeError("master_facilities.csv is empty")

    fieldnames = ensure_columns(list(rows[0].keys()))
    cache = load_cache()

    total = len(rows)
    geocoded = 0
    updated_cells = 0
    misses: List[Dict[str, str]] = []

    for i, r in enumerate(rows, 1):
        fid = norm(r.get("facility_id"))
        name = norm(r.get("name"))
        if not name:
            continue

        ward = norm(r.get("ward")) or WARD_HINT
        address_hint = norm(r.get("address"))

        need_geo = (
            is_blank(r.get("address")) or
            is_blank(r.get("lat")) or is_blank(r.get("lng")) or
            is_blank(r.get("map_url"))
        )

        if need_geo:
            out, used_q = lookup_nominatim(fid, name, ward, address_hint, cache)
            if out:
                if is_blank(r.get("address")) and out.get("address"):
                    r["address"] = str(out["address"]); updated_cells += 1
                if (is_blank(r.get("lat")) or is_blank(r.get("lng"))) and out.get("lat") is not None and out.get("lng") is not None:
                    r["lat"] = str(out["lat"]); r["lng"] = str(out["lng"]); updated_cells += 2
                if is_blank(r.get("map_url")) and out.get("map_url"):
                    r["map_url"] = str(out["map_url"]); updated_cells += 1
                geocoded += 1
            else:
                misses.append({"facility_id": fid, "name": name, "ward": ward, "query_tried": used_q})

        # nearest station / walk minutes
        try:
            lat = float(r.get("lat") or 0)
            lng = float(r.get("lng") or 0)
            if lat != 0 and lng != 0:
                if is_blank(r.get("nearest_station")) or is_blank(r.get("walk_minutes")):
                    st, wm = guess_nearest_station(lat, lng)
                    if is_blank(r.get("nearest_station")):
                        r["nearest_station"] = st; updated_cells += 1
                    if is_blank(r.get("walk_minutes")):
                        r["walk_minutes"] = str(wm); updated_cells += 1
        except Exception:
            pass

        if i % 50 == 0:
            print(f"processed {i}/{total} ... geocoded={geocoded} misses={len(misses)} updated_cells={updated_cells}")

    write_csv_file(OUT_CSV, rows, fieldnames)
    write_misses(misses)

    print("DONE. wrote:", OUT_CSV)
    print("total rows:", total)
    print("geocoded rows:", geocoded)
    print("misses:", len(misses), f"(see {MISSES_CSV.name})")
    print("updated cells:", updated_cells)

if __name__ == "__main__":
    main()
