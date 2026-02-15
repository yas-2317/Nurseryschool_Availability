#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import io
import json
import os
import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook

CITY_PAGE = "https://www.city.yokohama.lg.jp/kosodate-kyoiku/hoiku-yoji/shisetsu/riyou/info/nyusho-jokyo.html"

WARD_FILTER = (os.getenv("WARD_FILTER", "港北区") or "").strip() or None
MONTHS_BACK = int(os.getenv("MONTHS_BACK", "12"))
FORCE = (os.getenv("FORCE_BACKFILL", "0") == "1")

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
MASTER_CSV = DATA_DIR / "master_facilities.csv"


# ---------- small utils ----------
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


def month_floor(d: date) -> date:
    return date(d.year, d.month, 1)


def add_months(d: date, delta: int) -> date:
    y, m = d.year, d.month
    m2 = m + delta
    y += (m2 - 1) // 12
    m2 = (m2 - 1) % 12 + 1
    return date(y, m2, 1)


def iso(d: date) -> str:
    return d.isoformat()


def sanitize_header(header: List[str]) -> List[str]:
    out: List[str] = []
    seen: Dict[str, int] = {}
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


def reiwa_to_year(ry: int) -> int:
    # Reiwa 1 = 2019
    return 2018 + ry


def extract_reiwa_year_hint(text: str) -> Optional[int]:
    """
    '令和６年度' -> 6
    """
    if not text:
        return None
    t = str(text)
    t = t.translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    m = re.search(r"令和\s*([0-9]+)\s*年度", t)
    if m:
        return int(m.group(1))
    return None


def extract_month_from_text(text: str, ry_hint: Optional[int] = None) -> Optional[str]:
    """
    例:
      '【令和８年２月１日時点】' -> 2026-02-01
      '令和7年2月1日' -> 2025-02-01
      '2月1日' + ry_hint -> 年度から復元
    """
    if not text:
        return None
    t = str(text)
    z2h = str.maketrans("０１２３４５６７８９", "0123456789")
    t = t.translate(z2h)

    # 令和X年Y月1日
    m = re.search(r"令和\s*([0-9]+)\s*年\s*([0-9]+)\s*月\s*1\s*日", t)
    if m:
        ry = int(m.group(1))
        mm = int(m.group(2))
        y = reiwa_to_year(ry)
        return date(y, mm, 1).isoformat()

    # 西暦
    m = re.search(r"([0-9]{4})\s*年\s*([0-9]{1,2})\s*月\s*1\s*日", t)
    if m:
        y = int(m.group(1))
        mm = int(m.group(2))
        return date(y, mm, 1).isoformat()

    # 年なし（例: '2月1日'） + 年度ヒントで復元
    m = re.search(r"\b([0-9]{1,2})\s*月\s*1\s*日", t)
    if m and ry_hint is not None:
        mm = int(m.group(1))
        fy = reiwa_to_year(ry_hint)  # 年度開始年（4月の年）
        y = fy if mm >= 4 else fy + 1
        return date(y, mm, 1).isoformat()

    return None


def detect_month_from_rows(rows: List[Dict[str, str]]) -> Optional[str]:
    if not rows:
        return None
    for k in ("更新日", "更新年月日", "更新日時", "更新年月"):
        v = str(rows[0].get(k, "")).strip()
        if v:
            v = v[:10].replace("/", "-")
            try:
                y, m, _ = v.split("-")
                return date(int(y), int(m), 1).isoformat()
            except Exception:
                return None
    return None


# ---------- master ----------
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


def build_map_url(name: str, ward: str, address: str = "") -> str:
    q = " ".join([name, address, ward, "横浜市"]).strip()
    q = re.sub(r"\s+", " ", q)
    return f"https://www.google.com/maps/search/?api=1&query={q}"


# ---------- scraping ----------
def scrape_excel_links() -> List[Dict[str, Any]]:
    """
    ページから xls/xlsx/xlsm を全部拾う。
    ついでに親要素テキストから「令和◯年度」ヒントを取って持たせる。
    """
    html = requests.get(CITY_PAGE, timeout=30).text
    soup = BeautifulSoup(html, "html.parser")

    found: List[Dict[str, Any]] = []

    # a[href] から「.xls系を含む」ものを全部拾う（末尾一致にしない）
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        href_abs = href if href.startswith("http") else requests.compat.urljoin(CITY_PAGE, href)
        href_l = href_abs.lower()
        if (".xlsx" not in href_l) and (".xlsm" not in href_l) and (".xls" not in href_l):
            continue

        text = (a.get_text() or "").strip()
        parent_text = ""
        parent = a.find_parent(["tr", "li", "p", "div", "td"])
        if parent:
            parent_text = parent.get_text(" ", strip=True)

        ry_hint = extract_reiwa_year_hint(parent_text) or extract_reiwa_year_hint(text)

        found.append(
            {
                "url": href_abs,
                "text": text,
                "context": parent_text,
                "ry_hint": ry_hint,
            }
        )

    # 念のため本文から正規表現でも拾う（aタグ以外対策）
    for u in re.findall(r"https?://[^\s\"']+\.(?:xlsx|xlsm|xls)(?:\?[^\s\"']*)?", html, flags=re.I):
        found.append({"url": u, "text": "", "context": "", "ry_hint": None})

    # 重複除去（URL単位）
    uniq: List[Dict[str, Any]] = []
    seen = set()
    for d in found:
        u = d["url"]
        if u in seen:
            continue
        seen.add(u)
        uniq.append(d)

    if not uniq:
        raise RuntimeError("Excelリンクが拾えません（ページ仕様変更の可能性）")

    print("XLS links found:", len(uniq))
    return uniq


# ---------- Excel parsing ----------
def sheet_to_rows(ws) -> List[List[Any]]:
    rows: List[List[Any]] = []
    max_r = min(ws.max_row or 0, 7000)
    max_c = min(ws.max_column or 0, 140)
    for r in range(1, max_r + 1):
        row = []
        for c in range(1, max_c + 1):
            row.append(ws.cell(r, c).value)
        rows.append(row)
    return rows


def find_header_index(rows: List[List[Any]]) -> Optional[int]:
    keywords = ("施設", "区", "合計", "0歳", "０歳", "1歳", "１歳", "受入", "待ち", "児童")
    best_i, best_score = None, -1
    for i, row in enumerate(rows[:160]):
        cells = ["" if v is None else str(v) for v in row]
        nonempty = sum(1 for c in cells if c.strip() != "")
        has_kw = any(any(k in c for k in keywords) for c in cells)
        score = nonempty + (10 if has_kw else 0)
        if nonempty >= 5 and score > best_score:
            best_i, best_score = i, score
    return best_i


def parse_sheet(ws, ry_hint: Optional[int]) -> Tuple[Optional[str], List[Dict[str, str]]]:
    rows = sheet_to_rows(ws)

    # 月を探す（シート名→セルの順。探索範囲を広げる）
    month = extract_month_from_text(ws.title, ry_hint=ry_hint)

    if month is None:
        for r in rows[:40]:
            for v in r[:40]:
                month = extract_month_from_text("" if v is None else str(v), ry_hint=ry_hint)
                if month:
                    break
            if month:
                break

    hidx = find_header_index(rows)
    if hidx is None:
        return month, []

    header = sanitize_header([("" if v is None else str(v)) for v in rows[hidx]])
    out: List[Dict[str, str]] = []

    empty_streak = 0
    for r in rows[hidx + 1 :]:
        vals = [("" if v is None else str(v)) for v in r]
        if all(v.strip() == "" for v in vals):
            empty_streak += 1
            if empty_streak >= 10:
                break
            continue
        empty_streak = 0
        d = {header[i]: vals[i] if i < len(vals) else "" for i in range(len(header))}
        out.append(d)

    # rows内の更新日列から月が取れれば上書き
    m2 = detect_month_from_rows(out)
    if m2:
        month = m2

    return month, out


def detect_kind_from_workbook(wb) -> Optional[str]:
    """
    受入可能数 / 入所待ち人数 / 入所児童数 をExcel内の文字から判定
    """
    keys = [
        ("accept", ("受入可能", "受入可能数")),
        ("wait", ("入所待ち", "待ち人数")),
        ("enrolled", ("入所児童", "入所児童数")),
    ]
    for ws in wb.worksheets[:6]:
        max_r = min(ws.max_row or 0, 25)
        max_c = min(ws.max_column or 0, 25)
        for r in range(1, max_r + 1):
            for c in range(1, max_c + 1):
                v = ws.cell(r, c).value
                if v is None:
                    continue
                s = str(v)
                for kind, pats in keys:
                    if any(p in s for p in pats):
                        return kind
    return None


def kind_fallback_from_text(url: str, text: str, context: str) -> Optional[str]:
    blob = " ".join([url, text, context])
    if "入所待ち" in blob or "待ち人数" in blob or "machi" in blob.lower():
        return "wait"
    if "入所児童" in blob or "児童数" in blob:
        return "enrolled"
    if "受入可能" in blob:
        return "accept"
    return None


def read_xlsx(url: str, ry_hint: Optional[int], text: str, context: str) -> Tuple[str, Dict[str, List[Dict[str, str]]]]:
    """
    xlsx 1ファイル -> (kind, {month: rows}) を返す
    """
    print("download:", url, "ry_hint=", ry_hint)
    r = requests.get(url, timeout=180)
    r.raise_for_status()

    wb = load_workbook(io.BytesIO(r.content), data_only=True)

    kind = detect_kind_from_workbook(wb) or kind_fallback_from_text(url, text, context)
    if kind is None:
        raise RuntimeError(f"kind判定不能: {url}")

    mp: Dict[str, List[Dict[str, str]]] = {}
    for ws in wb.worksheets:
        month, rows = parse_sheet(ws, ry_hint=ry_hint)
        if month and rows:
            mp[month] = rows

    if mp:
        mn = min(mp.keys())
        mx = max(mp.keys())
        print("  parsed months:", len(mp), "range:", (mn, mx), "kind:", kind)
    else:
        print("  parsed months:", 0, "kind:", kind)

    return kind, mp


# ---------- column guessing / metrics ----------
def guess_facility_id_key(rows: List[Dict[str, str]]) -> str:
    if not rows:
        raise RuntimeError("rows empty")

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

    raise RuntimeError("施設番号列が見つかりません")


def index_by_key(rows: List[Dict[str, str]], key: str) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for r in rows:
        v = str(r.get(key, "")).strip()
        if v:
            out[v] = r
    return out


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


def build_age_groups(ar: Dict[str, str], wr: Dict[str, str], er: Dict[str, str]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
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

    g0, g1, g2 = ages_0_5["0"], ages_0_5["1"], ages_0_5["2"]
    g3, g4, g5 = ages_0_5["3"], ages_0_5["4"], ages_0_5["5"]

    w_35 = sum_opt(g3.get("wait"), g4.get("wait"), g5.get("wait"))
    cap_35 = sum_opt(g3.get("capacity_est"), g4.get("capacity_est"), g5.get("capacity_est"))

    age_groups = {
        "0": g0,
        "1": g1,
        "2": g2,
        "3-5": {
            "accept": sum_opt(g3.get("accept"), g4.get("accept"), g5.get("accept")),
            "wait": w_35,
            "enrolled": sum_opt(g3.get("enrolled"), g4.get("enrolled"), g5.get("enrolled")),
            "capacity_est": cap_35,
            "wait_per_capacity_est": ratio_opt(w_35, cap_35),
        },
    }
    return age_groups, ages_0_5


# ---------- main backfill ----------
def main() -> None:
    print("BACKFILL start. ward=", WARD_FILTER, "months_back=", MONTHS_BACK, "force=", FORCE)

    links = scrape_excel_links()
    master = load_master()
    target = norm(WARD_FILTER) if WARD_FILTER else None

    # kind -> month -> rows
    acc_by_month: Dict[str, List[Dict[str, str]]] = {}
    wai_by_month: Dict[str, List[Dict[str, str]]] = {}
    enr_by_month: Dict[str, List[Dict[str, str]]] = {}

    for d in links:
        url = d["url"]
        try:
            kind, mp = read_xlsx(url, d.get("ry_hint"), d.get("text", ""), d.get("context", ""))
            if kind == "accept":
                acc_by_month.update(mp)
            elif kind == "wait":
                wai_by_month.update(mp)
            elif kind == "enrolled":
                enr_by_month.update(mp)
        except Exception as e:
            print("WARN xlsx failed:", url, e)

    if not acc_by_month:
        raise RuntimeError("受入可能数の月次が1つも取れませんでした")

    # debug ranges
    acc_months = sorted(acc_by_month.keys())
    print("DEBUG months in accept:", len(acc_months), "range:", (acc_months[0], acc_months[-1]))

    end = month_floor(date.today())
    start = add_months(end, -(MONTHS_BACK - 1))
    want: List[str] = []
    cur = start
    while cur <= end:
        want.append(iso(cur))
        cur = add_months(cur, 1)

    available = [m for m in want if m in acc_by_month]
    missing = [m for m in want if m not in acc_by_month]
    print("want months:", len(want), "available:", len(available))
    if missing:
        print("missing months (accept):", missing)

    # months.json existing
    months_path = DATA_DIR / "months.json"
    existing_months: List[str] = []
    if months_path.exists():
        try:
            existing_months = json.loads(months_path.read_text(encoding="utf-8")).get("months", [])
        except Exception:
            existing_months = []

    for m in available:
        out_path = DATA_DIR / f"{m}.json"
        if out_path.exists() and not FORCE:
            print("skip exists:", out_path.name)
            continue

        accept_rows = acc_by_month.get(m, [])
        wait_rows = wai_by_month.get(m, [])
        enrolled_rows = enr_by_month.get(m, [])

        fid_a = guess_facility_id_key(accept_rows)
        A = index_by_key(accept_rows, fid_a)

        W: Dict[str, Dict[str, str]] = {}
        if wait_rows:
            try:
                fid_w = guess_facility_id_key(wait_rows)
                W = index_by_key(wait_rows, fid_w)
            except Exception:
                W = {}

        E: Dict[str, Dict[str, str]] = {}
        if enrolled_rows:
            try:
                fid_e = guess_facility_id_key(enrolled_rows)
                E = index_by_key(enrolled_rows, fid_e)
            except Exception:
                E = {}

        ward_key = pick_ward_key(accept_rows[0]) if accept_rows else None
        name_key = pick_name_key(accept_rows[0]) if accept_rows else None

        facilities: List[Dict[str, Any]] = []

        for fid, ar in A.items():
            ward = norm(ar.get(ward_key)) if ward_key else ""
            ward = ward.replace("横浜市", "")
            if target and target not in ward:
                continue

            wr = W.get(fid, {})
            er = E.get(fid, {})

            name = str(ar.get(name_key, "")).strip() if name_key else ""

            mm = master.get(fid, {})
            address = (mm.get("address") or "").strip()
            map_url = (mm.get("map_url") or "").strip() or build_map_url(name, ward, address)

            nearest_station = (mm.get("nearest_station") or mm.get("station") or mm.get("最寄り駅") or "").strip()
            walk_minutes = (mm.get("walk_minutes") or mm.get("徒歩") or mm.get("徒歩分") or "").strip()

            tot_accept = get_total(ar)
            tot_wait = get_total(wr) if wr else None
            tot_enrolled = get_total(er) if er else None
            cap_est = (tot_enrolled + tot_accept) if (tot_enrolled is not None and tot_accept is not None) else None

            age_groups, ages_0_5 = build_age_groups(ar, wr, er)

            facilities.append(
                {
                    "id": fid,
                    "name": name,
                    "ward": ward,
                    "address": address,
                    "map_url": map_url,
                    "nearest_station": nearest_station,
                    "walk_minutes": walk_minutes,
                    "updated": m,
                    "totals": {
                        "accept": tot_accept,
                        "wait": tot_wait,
                        "enrolled": tot_enrolled,
                        "capacity_est": cap_est,
                        "wait_per_capacity_est": ratio_opt(tot_wait, cap_est),
                    },
                    "age_groups": age_groups,
                    "ages_0_5": ages_0_5,
                }
            )

        out_path.write_text(
            json.dumps({"month": m, "ward": (WARD_FILTER or "横浜市"), "facilities": facilities}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print("wrote:", out_path.name, "facilities:", len(facilities))

    ms = set(existing_months)
    for m in available:
        p = DATA_DIR / f"{m}.json"
        if p.exists() and p.stat().st_size > 200:
            ms.add(m)

    months_path.write_text(json.dumps({"months": sorted(ms)}, ensure_ascii=False, indent=2), encoding="utf-8")
    print("updated months.json:", len(ms))


if __name__ == "__main__":
    main()
