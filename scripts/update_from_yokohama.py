#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
横浜市オープンデータ（保育所等の入所状況）から月次データを取得し、
data/YYYY-MM-01.json と data/months.json を更新します。

- 港北区だけ抽出（WARD_FILTERで変更可）
- 列名の揺れに強い（区名/施設番号/合計/年齢別など）
- 「-」等は 0 とみなす
- facilities が 0 件なら失敗扱い（壊れたデータをコミットしない）
- master_facilities.csv（任意）があれば住所や緯度経度などで上書き

使い方：
  python scripts/update_from_yokohama.py
"""

from __future__ import annotations

import csv
import json
import os
import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup


# データセットページ（横浜市オープンデータ）
DATASET_PAGE = "https://data.city.yokohama.lg.jp/dataset/kodomo_nyusho-jokyo"

# 港北区だけにしたい： "港北区"
# 全市にしたい： None または "" にする
WARD_FILTER = os.getenv("WARD_FILTER", "港北区").strip() or None

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

MASTER_CSV = DATA_DIR / "master_facilities.csv"


# ----------------------------
# Utils
# ----------------------------
def norm(s: Any) -> str:
    """文字の揺れを少しだけ吸収（全角スペース→半角、strip）"""
    if s is None:
        return ""
    return str(s).replace("　", " ").strip()


def to_int(x: Any) -> Optional[int]:
    """数字っぽいものを int に。空やnanはNone。「-」系は0。"""
    if x is None:
        return None
    s = norm(x)
    if s == "" or s.lower() == "nan":
        return None
    if s in ("-", "－", "‐", "-", "—", "―"):
        return 0
    try:
        # "3.0" みたいな表現もあるので float 経由
        return int(float(s))
    except Exception:
        return None


def detect_month(rows: List[Dict[str, str]]) -> str:
    """
    CSVに更新日列があればそこから YYYY-MM-DD を取る。
    無ければ当月1日を返す。
    """
    if rows:
        for k in ("更新日", "更新年月日", "更新日時", "更新年月"):
            if k in rows[0] and norm(rows[0].get(k)):
                v = norm(rows[0].get(k))[:10]
                # 2026/02/01 形式も想定
                v = v.replace("/", "-")
                return v
    today = date.today()
    return date(today.year, today.month, 1).isoformat()
