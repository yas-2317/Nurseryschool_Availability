#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import os
import re
from pathlib import Path

from pykakasi import kakasi

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MASTER_CSV = DATA_DIR / "master_facilities.csv"

WARD_FILTER = (os.getenv("WARD_FILTER", "") or "").strip() or None

# ひらがな変換器
_kks = kakasi()
_kks.setMode("J", "H")  # 漢字 -> ひらがな
_kks.setMode("K", "H")  # カタカナ -> ひらがな
_kks.setMode("H", "H")  # ひらがな -> ひらがな
_conv = _kks.getConverter()

def hira(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    s = _conv.do(s)
    # 記号類を軽く正規化（検索に強くする）
    s = s.replace("　", " ")
    s = re.sub(r"\s+", "", s)
    return s

def station_base(s: str) -> str:
    """ '日吉駅' -> '日吉' みたいに末尾の「駅」を落とす """
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("　", " ")
    s = s.strip()
    if s.endswith("駅"):
        s = s[:-1]
    return s.strip()

def main() -> None:
    if not MASTER_CSV.exists():
        raise SystemExit("master_facilities.csv not found: data/master_facilities.csv")

    rows = []
    with MASTER_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])

        # 無ければ列を追加
        for col in ["name_kana", "station_kana"]:
            if col not in fieldnames:
                fieldnames.append(col)

        for r in reader:
            # ward_filter があれば対象区だけ更新
            if WARD_FILTER:
                if (r.get("ward") or "").strip() != WARD_FILTER:
                    rows.append(r)
                    continue

            name = (r.get("name") or "").strip()
            st = (r.get("nearest_station") or "").strip()

            # 空欄だけ埋める（既に入ってるものは維持）
            if not (r.get("name_kana") or "").strip() and name:
                r["name_kana"] = hira(name)

            if not (r.get("station_kana") or "").strip() and st:
                r["station_kana"] = hira(station_base(st))

            rows.append(r)

    # 書き戻し
    tmp = MASTER_CSV.with_suffix(".csv.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            # 欠けたキーがあっても落ちないように
            out = {k: r.get(k, "") for k in fieldnames}
            w.writerow(out)

    tmp.replace(MASTER_CSV)
    print("DONE: updated kana columns in", str(MASTER_CSV))

if __name__ == "__main__":
    main()
