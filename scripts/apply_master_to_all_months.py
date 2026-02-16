#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MASTER_CSV = DATA_DIR / "master_facilities.csv"
MONTHS_JSON = DATA_DIR / "months.json"

WARD_FILTER = (os.getenv("WARD_FILTER", "") or "").strip() or None


def safe(x: Any) -> str:
    return "" if x is None else str(x)


def load_master() -> Dict[str, Dict[str, str]]:
    if not MASTER_CSV.exists():
        raise RuntimeError("data/master_facilities.csv が見つかりません")
    out: Dict[str, Dict[str, str]] = {}
    with MASTER_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            fid = safe(row.get("facility_id")).strip()
            if fid:
                out[fid] = {k: safe(v) for k, v in row.items()}
    return out


def load_months() -> List[str]:
    if not MONTHS_JSON.exists():
        raise RuntimeError("data/months.json が見つかりません")
    obj = json.loads(MONTHS_JSON.read_text(encoding="utf-8"))
    ms = obj.get("months") or []
    return [safe(m).strip() for m in ms if safe(m).strip()]


def as_int_str(x: str) -> Optional[str]:
    s = safe(x).strip()
    if s == "" or s.lower() == "null" or s == "-":
        return None
    try:
        return str(int(float(s)))
    except Exception:
        return None


def apply_master_to_facility(f: Dict[str, Any], m: Dict[str, str]) -> int:
    """
    f: month json facility object
    m: master row
    returns: number of fields updated
    """
    updated = 0

    mapping = {
        "address": "address",
        "lat": "lat",
        "lng": "lng",
        "map_url": "map_url",
        "facility_type": "facility_type",
        "phone": "phone",
        "website": "website",
        "notes": "notes",
        "nearest_station": "nearest_station",
        "name_kana": "name_kana",
        "station_kana": "station_kana",
    }

    for jkey, mkey in mapping.items():
        mv = safe(m.get(mkey)).strip()
        if mv == "":
            continue
        cur = safe(f.get(jkey)).strip()
        if cur != mv:
            f[jkey] = mv
            updated += 1

    # walk_minutes: normalize to int-string
    wm = as_int_str(safe(m.get("walk_minutes")))
    if wm is not None:
        cur = safe(f.get("walk_minutes")).strip()
        if cur != wm:
            f["walk_minutes"] = wm
            updated += 1

    return updated


def main() -> None:
    master = load_master()
    months = load_months()

    total_files = 0
    total_facilities = 0
    total_updates = 0

    for month in months:
        p = DATA_DIR / f"{month}.json"
        if not p.exists():
            continue

        obj = json.loads(p.read_text(encoding="utf-8"))
        facs = obj.get("facilities") or []

        if not isinstance(facs, list):
            continue

        changed = False
        file_updates = 0
        file_fac_count = 0

        for f in facs:
            if not isinstance(f, dict):
                continue
            fid = safe(f.get("id")).strip()
            ward = safe(f.get("ward")).strip()
            if WARD_FILTER and WARD_FILTER not in ward:
                continue

            m = master.get(fid)
            if not m:
                continue

            u = apply_master_to_facility(f, m)
            if u > 0:
                changed = True
                file_updates += u
            file_fac_count += 1

        if changed:
            p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

        total_files += 1
        total_facilities += file_fac_count
        total_updates += file_updates

        print(f"[{month}] facilities={file_fac_count} updates={file_updates} changed={changed}")

    print("DONE")
    print("  files:", total_files)
    print("  facilities_scanned:", total_facilities)
    print("  updated_cells:", total_updates)


if __name__ == "__main__":
    main()
