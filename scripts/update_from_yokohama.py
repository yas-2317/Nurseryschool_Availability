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
MASTER_CSV = DATA_DIR / "master_facilities.csv"


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


def sum_opt(*vals: Optional[int]) -> Optional[int]:
    xs = [v for v in vals if v is not None]
    return sum(xs) if xs else None


def ratio_opt(wait: Optional[int], cap: Optional[int]) -> Optional[float]:
    if wait is None or cap in (None, 0):
        return None
    return wait / cap


def detect_month(rows: List[Dict[str, str]]) -> str:
    if rows:
        for k in ("更新日", "更新年月日", "更新日時", "更新年月"):
            v = str(rows[0].get(k, "")).strip()
            if v:
                v = v[:10].replace("/", "-")
                # 月初へ丸め
                try:
                    y, m, _ = v.split("-")
                    return date(int(y), int(m), 1).isoformat()
                except Exception:
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

    lines = [ln for ln in text.splitlines() if ln is not None]

    def sanitize_header(header: List[str]) -> List[str]:
        out = []
        seen = {}
        for i, h in enumerate(header):
            h2 = (h or "").strip()
            if h2 == "":
                h2 = f"col{i}"
            if h2 in seen:
                seen[h2] += 1
                h2 = f"{h2}_{seen[h2]}"
            else:
                seen[h2] = 0
            out.append(h2)
        return out

    keywords = ("施設", "区", "合計", "0歳", "０歳", "1歳", "１歳", "待ち", "受入", "児童")
    best_idx = None
    best_score = -1
    preview_rows: List[List[str]] = []

    for i, row in enumerate(csv.reader(lines)):
        if i > 80:
            break
        preview_rows.append(row)
        nonempty = sum(1 for c in row if str(c).strip() != "")
        has_kw = any(any(k in str(c) for k in keywords) for c in row)
        score = nonempty + (10 if has_kw else 0)
        if nonempty >= 5 and score > best_score:
            best_score = score
            best_idx = i

    if best_idx is None:
        return list(csv.DictReader(lines))

    header = sanitize_header(preview_rows[best_idx])
    data_lines = lines[best_idx + 1 :]
    return list(csv.DictReader(data_lines, fieldnames=header))


def scrape_csv_urls() -> Dict[str, str]:
    html = requests.get(DATASET_PAGE, timeout=30).text
    soup = BeautifulSoup(html, "html.parser")

    links = [a.get("href", "") for a in soup.select("a[href]") if a.get("href", "").endswith(".csv")]
    if not links:
        links = re.findall(r"https?://[^\s\"']+\.csv", html)
    links = list(dict.fromkeys(links))

    best: Dict[str, str] = {}
    for url in links:
        if "0926_" in url:
            best["accept"] = url
        elif "0929_" in url:
            best["wait"] = url
        elif "0923_" in url:
            best["enrolled"] = url

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
    if "enrolled" not in best:
        for url in links:
            if ("入所児童" in url) or ("児童" in url):
                best["enrolled"] = url
                break

    if "accept" not in best or "wait" not in best:
        raise RuntimeError("CSVリンク抽出に失敗（ページ仕様変更の可能性）")

    print("CSV URLs:", best)
    return best


def load_master() -> Dict[str, Dict[str, str]]:
    if not MASTER_CSV.exists():
        return {}
    out: Dict[str, Dict[str, str]] = {}
    with MASTER_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            fid = (row.get("facility_id") or "").strip()
            if fid:
                out[fid] = row
    return out


def guess_facility_id_key(rows: List[Dict[str, str]]) -> str:
    if not rows:
        raise RuntimeError("CSVが空です")
    header = list(rows[0].keys())

    candidates = [
        "施設番号", "施設・事業所番号", "施設事業所番号", "事業所番号",
        "施設ID", "施設ＩＤ", "施設・事業所ID", "施設・事業所ＩＤ",
        "施設No", "施設Ｎｏ", "事業所No", "事業所Ｎｏ",
    ]
    for k in candidates:
        if k in rows[0]:
            return k

    patterns = ("番号", "ID", "ＩＤ", "No", "Ｎｏ", "NO", "ＮＯ")
    for k in header:
        if any(p in k for p in patterns) and ("施設" in k or "事業所" in k):
            return k

    N = min(200, len(rows))
    digit_re = re.compile(r"^\d{4,}$")
    best_key, best_score = None, -1
    for k in header:
        score = 0
        for i in range(N):
            v = str(rows[i].get(k, "")).strip()
            if digit_re.match(v):
                score += 1
        if score > best_score:
            best_key, best_score = k, score

    if best_key and best_score >= max(10, int(N * 0.30)):
        return best_key

    raise RuntimeError("施設番号列が見つかりません（列名・中身推定ともに失敗）")


def index_by_key(rows: List[Dict[str, str]], key: str) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for r in rows:
        v = str(r.get(key, "")).strip()
        if v:
            out[v] = r
    return out


def get_total(row: Dict[str, str]) -> Optional[int]:
    if not row:
        return None
    if "合計" in row and str(row.get("合計", "")).strip() != "":
        return to_int(row.get("合計"))
    for k in row.keys():
        if "合計" in k and str(row.get(k, "")).strip() != "":
            return to_int(row.get(k))
    return None


def get_age_value(row: Dict[str, str], age: int) -> Optional[int]:
    if not row:
        return None
    z = "０１２３４５"
    pats = [f"{age}歳児", f"{age}歳", z[age] + "歳児", z[age] + "歳"]
    for p in pats:
        if p in row and str(row.get(p, "")).strip() != "":
            return to_int(row.get(p))
    for k in row.keys():
        if any(p in k for p in pats) and str(row.get(k, "")).strip() != "":
            return to_int(row.get(k))
    return None


def build_map_url(name: str, ward: str, address: str = "", lat: str = "", lng: str = "") -> str:
    if lat and lng:
        return f"https://www.google.com/maps/search/?api=1&query={lat},{lng}"
    q = " ".join([name, address, ward, "横浜市"]).strip()
    q = re.sub(r"\s+", " ", q)
    return f"https://www.google.com/maps/search/?api=1&query={q}"


def pick_ward_key(row: Dict[str, str]) -> Optional[str]:
    for k in ("施設所在区", "所在区", "区名"):
        if k in row:
            return k
    for k in row.keys():
        if "区" in k:
            return k
    return None


def pick_name_key(row: Dict[str, str]) -> Optional[str]:
    for k in ("施設名", "施設・事業名", "施設・事業所名", "事業名"):
        if k in row:
            return k
    for k in row.keys():
        if "施設" in k and "区" not in k:
            return k
    return None


def main() -> None:
    print("START update_from_yokohama.py  WARD_FILTER=", WARD_FILTER)

    urls = scrape_csv_urls()
    accept_rows = read_csv_from_url(urls["accept"])
    wait_rows = read_csv_from_url(urls["wait"])

    enrolled_rows: List[Dict[str, str]] = []
    if "enrolled" in urls:
        try:
            enrolled_rows = read_csv_from_url(urls["enrolled"])
        except Exception as e:
            print("WARN: enrolled read failed:", e)

    month = detect_month(accept_rows)
    print("Detected month:", month)

    fid_key = guess_facility_id_key(accept_rows)
    A = index_by_key(accept_rows, fid_key)
    W = index_by_key(wait_rows, fid_key) if wait_rows and fid_key in wait_rows[0] else {}
    E = index_by_key(enrolled_rows, fid_key) if enrolled_rows and fid_key in enrolled_rows[0] else {}

    ward_key = pick_ward_key(accept_rows[0]) if accept_rows else None
    name_key = pick_name_key(accept_rows[0]) if accept_rows else None

    master = load_master()
    target = norm(WARD_FILTER) if WARD_FILTER else None

    facilities: List[Dict[str, Any]] = []

    for fid, ar in A.items():
        ward = norm(ar.get(ward_key)) if ward_key else ""
        ward = ward.replace("横浜市", "")

        if target and target not in ward:
            continue

        wr = W.get(fid, {})
        er = E.get(fid, {})

        name = str(ar.get(name_key, "")).strip() if name_key else ""

        m = master.get(fid, {})
        address = (m.get("address") or "").strip()
        lat = (m.get("lat") or "").strip()
        lng = (m.get("lng") or "").strip()
        map_url = (m.get("map_url") or "").strip() or build_map_url(name, ward, address, lat, lng)

        nearest_station = (m.get("nearest_station") or "").strip() or None
        walk_minutes = to_int((m.get("walk_minutes") or "").strip())

        phone = (m.get("phone") or "").strip() or None
        website = (m.get("website") or "").strip() or None
        facility_type = (m.get("facility_type") or "").strip() or None
        notes = (m.get("notes") or "").strip() or None

        tot_accept = get_total(ar)
        tot_wait = get_total(wr) if wr else None
        tot_enrolled = get_total(er) if er else None

        tot_capacity_est = (tot_enrolled + tot_accept) if (tot_enrolled is not None and tot_accept is not None) else None
        tot_wait_per_capacity_est = ratio_opt(tot_wait, tot_capacity_est)

        ages_0_5: Dict[str, Dict[str, Any]] = {}
        for i in range(6):
            a = get_age_value(ar, i)
            w = get_age_value(wr, i) if wr else None
            e = get_age_value(er, i) if er else None
            cap_est = (e + a) if (e is not None and a is not None) else None
            ages_0_5[str(i)] = {
                "accept": a,
                "wait": w,
                "enrolled": e,
                "capacity_est": cap_est,
                "wait_per_capacity_est": ratio_opt(w, cap_est),
            }

        g3, g4, g5 = ages_0_5.get("3", {}), ages_0_5.get("4", {}), ages_0_5.get("5", {})
        w_35 = sum_opt(g3.get("wait"), g4.get("wait"), g5.get("wait"))
        cap_35 = sum_opt(g3.get("capacity_est"), g4.get("capacity_est"), g5.get("capacity_est"))

        age_groups = {
            "0": ages_0_5.get("0", {}),
            "1": ages_0_5.get("1", {}),
            "2": ages_0_5.get("2", {}),
            "3-5": {
                "accept": sum_opt(g3.get("accept"), g4.get("accept"), g5.get("accept")),
                "wait": w_35,
                "enrolled": sum_opt(g3.get("enrolled"), g4.get("enrolled"), g5.get("enrolled")),
                "capacity_est": cap_35,
                "wait_per_capacity_est": ratio_opt(w_35, cap_35),
            },
        }

        facilities.append(
            {
                "id": fid,
                "name": name,
                "ward": ward,
                "address": address,
                "map_url": map_url,
                "nearest_station": nearest_station,
                "walk_minutes": walk_minutes,
                "phone": phone,
                "website": website,
                "facility_type": facility_type,
                "notes": notes,
                "updated": month,
                "totals": {
                    "accept": tot_accept,
                    "wait": tot_wait,
                    "enrolled": tot_enrolled,
                    "capacity_est": tot_capacity_est,
                    "wait_per_capacity_est": tot_wait_per_capacity_est,
                },
                "age_groups": age_groups,
                "ages_0_5": ages_0_5,
            }
        )

    if not facilities:
        raise RuntimeError("facilitiesが0件です（区フィルタ/列不一致の可能性）")

    month_path = DATA_DIR / f"{month}.json"
    month_path.write_text(
        json.dumps({"month": month, "ward": (WARD_FILTER or "横浜市"), "facilities": facilities}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    if month_path.stat().st_size < 200:
        raise RuntimeError("月次JSONが小さすぎます（生成失敗の可能性）")

    months_path = DATA_DIR / "months.json"
    months = {"months": [month]}
    if months_path.exists():
        try:
            old_txt = months_path.read_text(encoding="utf-8").strip()
            old = json.loads(old_txt) if old_txt else {}
            ms = set(old.get("months", []))
            ms.add(month)
            months["months"] = sorted(ms)
        except Exception:
            months = {"months": [month]}

    months_path.write_text(json.dumps(months, ensure_ascii=False, indent=2), encoding="utf-8")
    print("WROTE:", month_path.name, "and months.json")


if __name__ == "__main__":
    main()
