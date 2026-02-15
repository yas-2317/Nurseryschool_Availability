#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
import os
import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

DATASET_PAGE = "https://data.city.yokohama.lg.jp/dataset/kodomo_nyusho-jokyo"

WARD_FILTER = (os.getenv("WARD_FILTER", "港北区") or "").strip()
if WARD_FILTER == "":
    WARD_FILTER = None

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def norm(s: Any) -> str:
    if s is None:
        return ""
    x = str(s).replace("　", " ")
    x = re.sub(r"\s+", "", x)
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
            v = str(rows[0].get(k, "")).strip()
            if v:
                return v[:10].replace("/", "-")
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
    soup = BeautifulSoup(html, "html.parser")

    links = [a.get("href", "") for a in soup.select("a[href]") if a.get("href", "").endswith(".csv")]
    if not links:
        links = re.findall(r"https?://[^\s\"']+\.csv", html)
    links = list(dict.fromkeys(links))

    best: Dict[str, str] = {}

    # 既知ID（あるなら最優先）
    for url in links:
        if "0926_" in url:
            best["accept"] = url
        elif "0929_" in url:
            best["wait"] = url

    # キーワード推定（保険）
    if "accept" not in best:
        for url in links:
            if ("受入" in url) or ("入所可能" in url):
                best["accept"] = url
                break

    if "wait" not in best:
        for url in links:
            if "待ち" in url:
                best["wait"] = url
                break

    if "accept" not in best or "wait" not in best:
        raise RuntimeError("CSVリンク抽出に失敗（ページ仕様変更の可能性）")

    print("CSV URLs:", best)
    return best
