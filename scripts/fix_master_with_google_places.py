#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# ------------------------
# Paths / Env
# ------------------------
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MASTER_CSV = DATA_DIR / "master_facilities.csv"

API_KEY = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
if not API_KEY:
    raise SystemExit("ERROR: GOOGLE_MAPS_API_KEY is required (set env or GitHub Secrets)")

# 任意: 港北区だけ等に絞る
WARD_FILTER = (os.getenv("WARD_FILTER") or "").strip() or None

# 叩きすぎ防止（秒）
SLEEP_SEC = float(os.getenv("GOOGLE_API_SLEEP_SEC", "0.1"))

# 更新対象を「怪しい行だけ」にする（1推奨）
ONLY_BAD_ROWS = (os.getenv("ONLY_BAD_ROWS", "1") == "1")

# 住所の一致チェックを厳格にする（1推奨）
STRICT_ADDRESS_CHECK = (os.getenv("STRICT_ADDRESS_CHECK", "1") == "1")

# phone/website/map_url を上書きするか（通常は空欄のみ埋めるのが安全）
OVERWRITE_PHONE = (os.getenv("OVERWRITE_PHONE", "0") == "1")
OVERWRITE_WEBSITE = (os.getenv("OVERWRITE_WEBSITE", "0") == "1")
OVERWRITE_MAP_URL = (os.getenv("OVERWRITE_MAP_URL", "0") == "1")

# address / latlng を上書きするか（通常は怪しい行のみ更新でOK）
OVERWRITE_ADDRESS = (os.getenv("OVERWRITE_ADDRESS", "1") == "1")
OVERWRITE_LATLNG = (os.getenv("OVERWRITE_LATLNG", "1") == "1")

# 一回の実行で更新する最大件数（コスト制御）
MAX_UPDATES = int(os.getenv("MAX_UPDATES", "999999"))

LANG = "ja"
REGION = "jp"

# Places API (Legacy Web Service endpoints)
FIND_PLACE_URL = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"


# ------------------------
# Helpers
# ------------------------
def norm(s: Any) -> str:
    s = "" if s is None else str(s)
    s = s.replace("　", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def norm_key(s: Any) -> str:
    s = norm(s).lower()
    s = s.replace("（", "(").replace("）", ")")
    s = re.sub(r"[()\[\]「」『』・,，\.。、】【]", "", s)
    # 施設名の揺れ吸収（必要に応じて追加）
    s = s.replace("認定こども園", "").replace("こども園", "")
    s = s.replace("保育園", "").replace("保育所", "")
    s = s.replace("横浜", "").replace("市", "").replace("区", "")
    s = re.sub(r"\s+", "", s)
    return s


def to_int(x: Any) -> Optional[int]:
    s = norm(x)
    if s == "" or s.lower() == "nan":
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def safe_get(d: dict, path: List[str], default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def looks_bad_address(address: str, ward: str) -> bool:
    a = norm(address)
    w = norm(ward)
    if a == "":
        return True
    # 最低限：横浜市を含む
    if "横浜市" not in a:
        return True
    # ward があれば含む
    if w and w not in a:
        return True
    return False


def looks_missing_latlng(lat: str, lng: str) -> bool:
    return norm(lat) == "" or norm(lng) == ""


def request_json(url: str, params: dict, timeout: int = 30) -> dict:
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def find_place(query: str) -> List[dict]:
    params = {
        "input": query,
        "inputtype": "textquery",
        "fields": "place_id,name,formatted_address,geometry",
        "language": LANG,
        "region": REGION,
        "key": API_KEY,
    }
    js = request_json(FIND_PLACE_URL, params=params, timeout=30)
    if js.get("status") != "OK":
        return []
    return js.get("candidates", []) or []


def text_search(query: str) -> List[dict]:
    params = {
        "query": query,
        "language": LANG,
        "region": REGION,
        "key": API_KEY,
    }
    js = request_json(TEXT_SEARCH_URL, params=params, timeout=30)
    if js.get("status") != "OK":
        return []
    return js.get("results", []) or []


def place_details(place_id: str) -> Optional[dict]:
    params = {
        "place_id": place_id,
        # 欲しいものだけ（コスト/レスポンス節約）
        "fields": "place_id,name,formatted_address,geometry/location,formatted_phone_number,website,url",
        "language": LANG,
        "region": REGION,
        "key": API_KEY,
    }
    js = request_json(DETAILS_URL, params=params, timeout=30)
    if js.get("status") != "OK":
        return None
    return js.get("result")


def choose_best(target_name: str, candidates: List[dict]) -> Optional[dict]:
    if not candidates:
        return None
    t = norm_key(target_name)

    best = None
    best_score = -10**9
    for c in candidates[:7]:
        cn = norm_key(c.get("name", ""))
        score = 0

        if t and cn:
            # 片方が片方を含むなら強く加点
            if t in cn or cn in t:
                score += 80
            # 共有文字（雑だけど意外と効く）
            score += len(set(t) & set(cn))

        # 住所がある候補を少し優遇
        if c.get("formatted_address"):
            score += 5
        # geometryがある候補も優遇
        if safe_get(c, ["geometry", "location", "lat"]) is not None:
            score += 2

        if score > best_score:
            best_score = score
            best = c

    return best


def ensure_columns(fieldnames: List[str], required: List[str]) -> List[str]:
    s = set(fieldnames)
    for c in required:
        if c not in s:
            fieldnames.append(c)
            s.add(c)
    return fieldnames


def read_master(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        fieldnames = r.fieldnames or []
        rows = [dict(x) for x in r]
    return fieldnames, rows


def write_master(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def should_update_address(new_addr: str, ward: str) -> bool:
    a = norm(new_addr)
    w = norm(ward)
    if a == "":
        return False
    if STRICT_ADDRESS_CHECK:
        if "横浜市" not in a:
            return False
        if w and w not in a:
            return False
    return True


def build_query(name: str, ward: str, address_hint: str = "") -> str:
    parts = [name]
    if address_hint:
        parts.append(address_hint)
    if ward:
        parts.append(ward)
    parts.append("横浜市")
    parts.append("保育園")
    return norm(" ".join(parts))


# ------------------------
# Main
# ------------------------
def main() -> None:
    if not MASTER_CSV.exists():
        raise SystemExit(f"ERROR: not found: {MASTER_CSV}")

    fieldnames, rows = read_master(MASTER_CSV)

    required_cols = [
        "facility_id", "name", "ward",
        "address", "lat", "lng",
        "facility_type", "phone", "website", "notes",
        "nearest_station", "walk_minutes",
        "map_url",
    ]
    fieldnames = ensure_columns(fieldnames, required_cols)

    misses_path = DATA_DIR / "geocode_misses.csv"
    updates_path = DATA_DIR / "master_facilities_updates.csv"

    miss_rows: List[Dict[str, str]] = []
    update_rows: List[Dict[str, str]] = []

    updated = 0
    checked = 0

    for row in rows:
        fid = norm(row.get("facility_id", ""))
        name = norm(row.get("name", ""))
        ward = norm(row.get("ward", ""))

        if not name:
            continue

        if WARD_FILTER and norm(WARD_FILTER) not in ward:
            continue

        address = norm(row.get("address", ""))
        lat = norm(row.get("lat", ""))
        lng = norm(row.get("lng", ""))

        bad_addr = looks_bad_address(address, ward)
        miss_ll = looks_missing_latlng(lat, lng)

        if ONLY_BAD_ROWS and not (bad_addr or miss_ll):
            continue

        checked += 1
        if updated >= MAX_UPDATES:
            break

        # クエリ（住所ヒントがあると精度が上がるが、誤住所なら逆効果なので控えめに）
        addr_hint = "" if bad_addr else address
        q = build_query(name, ward, addr_hint)

        # 1) Find Place
        candidates = find_place(q)
        best = choose_best(name, candidates)

        # 2) 保険：Text Search
        if best is None:
            candidates2 = text_search(q)
            best = choose_best(name, candidates2)

        if best is None:
            miss_rows.append({
                "facility_id": fid, "name": name, "ward": ward,
                "query_tried": q, "reason": "no_candidates"
            })
            time.sleep(SLEEP_SEC)
            continue

        pid = best.get("place_id") or ""
        if not pid:
            miss_rows.append({
                "facility_id": fid, "name": name, "ward": ward,
                "query_tried": q, "reason": "no_place_id"
            })
            time.sleep(SLEEP_SEC)
            continue

        det = place_details(pid)
        if not det:
            miss_rows.append({
                "facility_id": fid, "name": name, "ward": ward,
                "query_tried": q, "reason": "details_failed"
            })
            time.sleep(SLEEP_SEC)
            continue

        new_addr = norm(det.get("formatted_address", ""))
        loc_lat = safe_get(det, ["geometry", "location", "lat"], None)
        loc_lng = safe_get(det, ["geometry", "location", "lng"], None)
        new_phone = norm(det.get("formatted_phone_number", ""))
        new_web = norm(det.get("website", ""))
        new_url = norm(det.get("url", ""))

        # 誤爆防止
        if not should_update_address(new_addr, ward):
            miss_rows.append({
                "facility_id": fid, "name": name, "ward": ward,
                "query_tried": q, "reason": f"addr_mismatch:{new_addr}"
            })
            time.sleep(SLEEP_SEC)
            continue

        # before snapshot for diff log
        before = {
            "facility_id": fid,
            "name": name,
            "ward": ward,
            "address_before": address,
            "lat_before": lat,
            "lng_before": lng,
            "phone_before": norm(row.get("phone", "")),
            "website_before": norm(row.get("website", "")),
            "map_url_before": norm(row.get("map_url", "")),
            "query": q,
            "place_id": pid,
        }

        # Update fields
        if OVERWRITE_ADDRESS:
            row["address"] = new_addr

        if OVERWRITE_LATLNG and (loc_lat is not None) and (loc_lng is not None):
            row["lat"] = str(loc_lat)
            row["lng"] = str(loc_lng)

        if OVERWRITE_PHONE or (not norm(row.get("phone", ""))):
            if new_phone:
                row["phone"] = new_phone

        if OVERWRITE_WEBSITE or (not norm(row.get("website", ""))):
            if new_web:
                row["website"] = new_web

        if OVERWRITE_MAP_URL or (not norm(row.get("map_url", ""))):
            if new_url:
                row["map_url"] = new_url

        after = {
            "address_after": norm(row.get("address", "")),
            "lat_after": norm(row.get("lat", "")),
            "lng_after": norm(row.get("lng", "")),
            "phone_after": norm(row.get("phone", "")),
            "website_after": norm(row.get("website", "")),
            "map_url_after": norm(row.get("map_url", "")),
        }

        update_rows.append({**before, **after})
        updated += 1
        time.sleep(SLEEP_SEC)

    # write master
    write_master(MASTER_CSV, fieldnames, rows)

    # write updates log
    if update_rows:
        with updates_path.open("w", encoding="utf-8", newline="") as f:
            cols = [
                "facility_id", "name", "ward",
                "query", "place_id",
                "address_before", "address_after",
                "lat_before", "lat_after",
                "lng_before", "lng_after",
                "phone_before", "phone_after",
                "website_before", "website_after",
                "map_url_before", "map_url_after",
            ]
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in update_rows:
                w.writerow({k: r.get(k, "") for k in cols})

    # write misses
    if miss_rows:
        with misses_path.open("w", encoding="utf-8", newline="") as f:
            cols = ["facility_id", "name", "ward", "query_tried", "reason"]
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in miss_rows:
                w.writerow({k: r.get(k, "") for k in cols})

    print("DONE")
    print("timestamp:", datetime.now().isoformat(timespec="seconds"))
    print("total rows:", len(rows))
    print("checked:", checked)
    print("updated:", updated, f"(see {updates_path.name})" if update_rows else "")
    print("misses:", len(miss_rows), f"(see {misses_path.name})" if miss_rows else "")


if __name__ == "__main__":
    main()
