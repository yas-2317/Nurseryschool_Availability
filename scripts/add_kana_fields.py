#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MASTER_CSV = DATA_DIR / "master_facilities.csv"

WARD_FILTER = (os.getenv("WARD_FILTER", "") or "").strip()
KANA_OVERWRITE = (os.getenv("KANA_OVERWRITE", "0") == "1")

# ----------------------------
# Kana / normalize helpers
# ----------------------------

# Katakana -> Hiragana
def kata_to_hira(s: str) -> str:
    out = []
    for ch in s:
        o = ord(ch)
        # Katakana block
        if 0x30A1 <= o <= 0x30F6:
            out.append(chr(o - 0x60))
        else:
            out.append(ch)
    return "".join(out)

# Zenkaku -> Hankaku for ASCII-ish
Z2H = str.maketrans("０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ－ー　",  # noqa
                    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-- ")

def norm_spaces(s: str) -> str:
    s = s.translate(Z2H)
    s = s.replace("\u3000", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

# keep hiragana only (and small vowels etc)
HIRAGANA_RE = re.compile(r"[ぁ-ゖー]+")

def extract_hiragana_like(s: str) -> str:
    """
    Extract kana chunks (hiragana/katakana) and convert to hiragana.
    If none, return empty string.
    """
    if not s:
        return ""
    s = norm_spaces(str(s))
    s = kata_to_hira(s)
    parts = HIRAGANA_RE.findall(s)
    return "".join(parts)

# remove common noise tokens from nursery names
NOISE_WORDS = [
    "横浜市", "港北区",
    "保育園", "保育えん", "ほいくえん",
    "保育所", "ほいくしょ",
    "こども園", "認定こども園",
    "幼稚園", "ようちえん",
    "小規模", "事業所内", "家庭的",
    "分園", "本園",
    "ナーサリー", "にゅーさりー",
    "キッズ", "きっず",
    "園", "えん",
]

def strip_noise(s: str) -> str:
    x = norm_spaces(s)
    # normalize symbols
    x = x.replace("・", " ").replace("／", " ").replace("/", " ")
    x = x.replace("（", " ").replace("）", " ")
    x = x.replace("(", " ").replace(")", " ")
    x = re.sub(r"[^\wぁ-ゖァ-ヶー ]+", " ", x)
    x = re.sub(r"\s+", " ", x).strip()

    # remove noise words (hiragana + kanji forms both)
    y = x
    for w in NOISE_WORDS:
        y = y.replace(w, " ")
    y = re.sub(r"\s+", " ", y).strip()
    return y

# optional: small built-in station reading hints (expand over time)
# key: kanji station name (without "駅"), value: hira reading
STATION_KANA_DICT: Dict[str, str] = {
    "日吉": "ひよし",
    "綱島": "つなしま",
    "大倉山": "おおくらやま",
    "菊名": "きくな",
    "新横浜": "しんよこはま",
    "妙蓮寺": "みょうれんじ",
    "新羽": "にっぱ",
    "北新横浜": "きたしんよこはま",
    "高田": "たかた",
    "東山田": "ひがしやまた",
    "日吉本町": "ひよしほんちょう",
}

# optional: nursery name reading hints (expand over time)
# key: exact nursery name, value: hira reading
NAME_KANA_DICT: Dict[str, str] = {
    # 例）必要に応じて増やす
    "ベネッセ 日吉保育園": "べねっせ ひよし",
    "ベネッセ　日吉保育園": "べねっせ ひよし",
    "ベネッセ 新横浜保育園": "べねっせ しんよこはま",
    "ベネッセ　新横浜保育園": "べねっせ しんよこはま",
}

def build_search_kana_from_text(text: str) -> str:
    """
    APIなし前提の “検索用かな” を作る。
    - 既にかなが含まれていれば抽出してひらがな化
    - 漢字しかない場合は空になるので、空のまま（=辞書/手入力で補う前提）
    """
    if not text:
        return ""
    # exact dict hit first
    if text in NAME_KANA_DICT:
        return NAME_KANA_DICT[text].replace(" ", "")

    base = strip_noise(text)
    kana = extract_hiragana_like(base)
    kana = kana.replace("ー", "")  # long vowel mark often not needed for search
    kana = kana.replace(" ", "")
    return kana

def build_station_kana(nearest_station: str) -> str:
    """
    nearest_station から station_kana を作る
    - "◯◯駅" を除去
    - dict（漢字駅→かな）を優先
    - かなが混ざっていれば抽出
    """
    if not nearest_station:
        return ""

    s = norm_spaces(nearest_station)
    s = s.replace("駅", "").strip()
    s = s.replace("（", " ").replace("）", " ")
    s = re.sub(r"\s+", " ", s).strip()

    # dict exact / token match
    if s in STATION_KANA_DICT:
        return STATION_KANA_DICT[s].replace(" ", "")

    # if contains kana
    kana = extract_hiragana_like(s).replace("ー", "").replace(" ", "")
    if kana:
        return kana

    # try token-level dict (e.g., "日吉本町" etc.)
    for k, v in STATION_KANA_DICT.items():
        if k in s:
            return v.replace(" ", "")

    return ""

def should_apply(ward: str) -> bool:
    if not WARD_FILTER:
        return True
    w = (ward or "").strip()
    return WARD_FILTER in w

# ----------------------------
# CSV IO
# ----------------------------

def read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = [dict(r) for r in reader]
    return fieldnames, rows

def write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    tmp = path.with_suffix(".tmp.csv")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    tmp.replace(path)

def ensure_cols(fieldnames: List[str], cols: List[str]) -> List[str]:
    out = list(fieldnames)
    for c in cols:
        if c not in out:
            out.append(c)
    return out

# ----------------------------
# Main
# ----------------------------

def main() -> None:
    if not MASTER_CSV.exists():
        raise SystemExit(f"master csv not found: {MASTER_CSV}")

    fieldnames, rows = read_csv(MASTER_CSV)
    fieldnames = ensure_cols(fieldnames, ["name_kana", "station_kana"])

    upd = 0
    touched = 0

    for r in rows:
        if not should_apply(r.get("ward", "")):
            continue

        name = (r.get("name") or "").strip()
        station = (r.get("nearest_station") or "").strip()

        # name_kana
        cur_name_kana = (r.get("name_kana") or "").strip()
        if KANA_OVERWRITE or (cur_name_kana == ""):
            gen = build_search_kana_from_text(name)
            if gen and gen != cur_name_kana:
                r["name_kana"] = gen
                upd += 1
                touched += 1
            elif cur_name_kana == "" and not gen:
                # leave empty; user can fill / dict expand later
                pass

        # station_kana
        cur_station_kana = (r.get("station_kana") or "").strip()
        if KANA_OVERWRITE or (cur_station_kana == ""):
            gen_s = build_station_kana(station)
            if gen_s and gen_s != cur_station_kana:
                r["station_kana"] = gen_s
                upd += 1
                touched += 1
            elif cur_station_kana == "" and not gen_s:
                pass

    write_csv(MASTER_CSV, fieldnames, rows)

    print("DONE add_kana_fields.py")
    print("WARD_FILTER=", WARD_FILTER if WARD_FILTER else "(none)")
    print("KANA_OVERWRITE=", int(KANA_OVERWRITE))
    print("rows=", len(rows))
    print("updated_cells=", upd)
    print("touched_rows=", touched)
    print("wrote:", str(MASTER_CSV))

if __name__ == "__main__":
    main()
