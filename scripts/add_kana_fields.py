#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import os
import re
from pathlib import Path
from typing import Dict, List

from pykakasi import kakasi

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MASTER_CSV = DATA_DIR / "master_facilities.csv"

WARD_FILTER = (os.getenv("WARD_FILTER") or "").strip() or None
OVERWRITE_KANA = (os.getenv("OVERWRITE_KANA", "0") == "1")  # 0:空欄だけ / 1:上書き

# ひらがなで持つ（検索が楽）
_kks = kakasi()
_kks.setMode("J", "H")  # 漢字→ひらがな
_kks.setMode("K", "H")  # カタカナ→ひらがな
_kks.setMode("H", "H")  # ひらがな→ひらがな
_conv = _kks.getConverter()

def to_hira(s: str) -> str:
    if not s:
        return ""
    t = str(s)
    t = re.sub(r"\s+", "", t.replace("　", ""))
    t = _conv.do(t)
    # 記号類は落として検索ブレを減らす（必要なら調整）
    t = re.sub(r"[・()（）［］\[\]{}「」『』【】\-ー—‐/／.,。、]", "", t)
    return t

def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def write_csv(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: (r.get(k, "") if r.get(k, "") is not None else "") for k in fieldnames})

def ensure_cols(fieldnames: List[str], cols: List[str]) -> List[str]:
    for c in cols:
        if c not in fieldnames:
            fieldnames.append(c)
    return fieldnames

def main() -> None:
    if not MASTER_CSV.exists():
        raise RuntimeError("data/master_facilities.csv が見つかりません")

    rows = read_csv(MASTER_CSV)
    if not rows:
        raise RuntimeError("master_facilities.csv が空です")

    fieldnames = list(rows[0].keys())
    fieldnames = ensure_cols(fieldnames, ["name_kana", "station_kana"])

    target = WARD_FILTER.strip() if WARD_FILTER else None

    updated_cells = 0
    updated_rows = 0

    for r in rows:
        ward = (r.get("ward") or "").strip()
        if target and target not in ward:
            continue

        name = (r.get("name") or "").strip()
        st = (r.get("nearest_station") or "").strip()

        nk_cur = (r.get("name_kana") or "").strip()
        sk_cur = (r.get("station_kana") or "").strip()

        nk_new = to_hira(name)
        sk_new = to_hira(st)

        if (nk_cur == "" and nk_new) or (OVERWRITE_KANA and nk_new and nk_cur != nk_new):
            r["name_kana"] = nk_new
            updated_cells += 1

        if (sk_cur == "" and sk_new) or (OVERWRITE_KANA and sk_new and sk_cur != sk_new):
            r["station_kana"] = sk_new
            updated_cells += 1

        updated_rows += 1

    write_csv(MASTER_CSV, rows, fieldnames)

    print("DONE add_kana_fields.py")
    print("WARD_FILTER=", WARD_FILTER, "OVERWRITE_KANA=", OVERWRITE_KANA)
    print("updated rows:", updated_rows, "updated cells:", updated_cells)
    print("wrote:", str(MASTER_CSV))

if __name__ == "__main__":
    main()
