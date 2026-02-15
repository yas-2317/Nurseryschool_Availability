#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

DATASET_PAGE = "https://data.city.yokohama.lg.jp/dataset/kodomo_nyusho-jokyo"

# 港北区だけ： "港北区"
# 全市： "" にする（Actionsのenvでも上書き可）
WARD_FILTER = (os.getenv("WARD_FILTER", "港北区") or "").strip()
if WARD_FILTER == "":
    WARD_FILTER = None

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

MASTER_CSV = DATA_DIR / "master_facilities.csv"


def norm(s: Any) -> str:
    """空白除去＋全角空白潰し"""
    if s is None:
        return ""
    x = str(s).replace("　", " ")
    x = re.sub(r"\s+", "", x)  # 全空白削除
    return x.strip()


def to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return None
    if s in ("-", "－", "‐", "—", "―"):
        return 0
    try:
        return int(float(s))
    except Exception:
        return None


def detect_month(rows: List[Dict[str, str]]) -> str:
    if rows:
        for k in ("更新日", "更新年月日", "更新日時", "更新年月"):
            if k in rows[0] and str(rows[0].get(k, "")).strip():
                v = str(rows[0].get(k, "")).strip()[:10].replace("/", "-")
                return v
    today = date.today()
    return date(today.year, today.month, 1).isoformat()


def read_csv_from_url(url: str) -> List[Dict[str, str]]:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    for enc in ("cp932", "shift_jis", "utf-8-sig", "utf-8"):
        try:
            text = r.content.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = r.text
    return list(csv.DictReader(text.splitlines()))


def scrape_csv_urls() -> Dict[str, str]:
    html = requests.get(DATASET_PAGE, timeout=30).text
    soup = Beautif
