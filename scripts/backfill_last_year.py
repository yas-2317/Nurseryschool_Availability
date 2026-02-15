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
    out = []
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


def extract_month_from_text(text: str) -> Optional[str]:
    """
    例: '【令和８年２月１日時点】' → 2026-02-01
    """
    if not text:
        return None
    t = str(text)
    z2h = str.maketrans("０１２３４５６７８９", "0123456789")
    t = t.translate(z2h)

    m = re.search(r"令和\s*([0-9]+)\s*年\s*([0-9]+)\s*月\s*1\s*日", t)
    if m:
        ry = int(m.group(1))
        mm = int(m.group(2))
        y = 2018 + ry  # Reiwa 1 = 2019
        return date(y, mm, 1).isoformat()

    m = re.search(r"([0-9]{4})\s*年\s*([0-9]{1,2})\s*月\s*1\s*日", t)
    if m:
        y = int(m.group(1))
        mm = int(m.group(2))
        return date(y, mm, 1).isoformat()

    return None


def detect_month_from_rows(rows: List[Dict[str, str]]) -> Optional[str]:
    if not rows:
        return None
    for k in ("更新日", "更新年月日", "更新日時", "更新年月"):
        v = str(rows[0].get(k, "")).strip()
        if v:
            v = v[:10].replace("/", "-")
            # だいたい "YYYY-MM-DD" で来る想定。月初へ丸める
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
def scrape_excel_urls() -> Dict[str, List[str]]:
    """
    横浜市ページから Excel リンク（.xls/.xlsx/.xlsm）を頑丈に拾って分類する
    """
    html = requests.get(CITY_PAGE, timeout=30).text
    soup = BeautifulSoup(html, "html.parser")

    # 1) a[href] から「.xls系を含む」ものを全部拾う（末尾一致にしない）
    found: List[Tuple[str, str]] = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        href_abs = href if href.startswith("http") else requests.compat.urljoin(CITY_PAGE, href)
        href_l = href_abs.lower()
        if (".xlsx" in href_l) or (".xlsm" in href_l) or (".xls" in href_l):
            text = (a.get_text() or "").strip()
            found.append((href_abs, text))

    # 2) 念のため、本文から正規表現でも拾う（aタグ以外で埋まってる場合対策）
    for u in re.findall(r"https?://[^\s\"']+\.(?:xlsx|xlsm|xls)(?:\?[^\s\"']*)?", html, flags=re.I):
        found.append((u, ""))

    # 重複除去（順序維持）
    seen = set()
    uniq: List[Tuple[str, str]] = []
    for u, t in found:
        if u not in seen:
            seen.add(u)
            uniq.append((u, t))

    urls: Dict[str, List[str]] = {"accept": [], "wait": [], "enrolled": []}

    # 3) まずリンクテキストで分類（ページ上は「受入可能数（エクセル）」等と明示されている） :contentReference[oaicite:1]{index=1}
    for u, t in uniq:
        if "入所児童" in t:
            urls["enrolled"].append(u)
        elif "受入可能" in t:
            urls["accept"].append(u)
        elif ("入所待ち" in t) or ("待ち人数" in t):
            urls["wait"].append(u)

    # 4) テキストが取れない/揺れる場合の保険：URL文字列で分類（ファイル名に番号が入ることが多い）
    def push_if(kind: str, pred):
        if urls[kind]:
            return
        for u, _ in uniq:
            ul = u.lower()
            if pred(ul):
                urls[kind].append(u)

    push_if("accept",   lambda ul: ("受入" in ul) or ("ukire" in ul) or ("0932_" in ul))
    push_if("wait",     lambda ul: ("待ち" in ul) or ("mati" in ul) or ("0933_" in ul) or ("0929_" in ul))
    push_if("enrolled", lambda ul: ("児童" in ul) or ("jido" in ul) or ("0934_" in ul) or ("0923_" in ul))

    # 最後に重複除去
    for k in list(urls.keys()):
        s = set()
        out = []
        for u in urls[k]:
            if u not in s:
                s.add(u)
                out.append(u)
        urls[k] = out

    if not urls["accept"] or not urls["wait"]:
        # デバッグ用に候補を少し出す
        sample = [u for u, _ in uniq][:10]
        raise RuntimeError(f"Excelリンクが拾えません（候補={len(uniq)}件、例={sample}）")

    print("XLSX/XLS links:", {k: len(v) for k, v in urls.items()})
    return urls

    # unique preserve order
    for k in list(urls.keys()):
        seen = set()
        out = []
        for u in urls[k]:
            if u not in seen:
                seen.add(u)
                out.append(u)
        urls[k] = out

    if not urls["accept"] or not urls["wait"]:
        raise RuntimeError("Excelリンクが拾えません（ページ仕様変更の可能性）")

    print("XLSX links:", {k: len(v) for k, v in urls.items()})
    return urls


# ---------- Excel parsing ----------
def sheet_to_rows(ws) -> List[List[Any]]:
    rows: List[List[Any]] = []
    max_r = min(ws.max_row or 0, 6000)
    max_c = min(ws.max_column or 0, 120)
    for r in range(1, max_r + 1):
        row = []
        for c in range(1, max_c + 1):
            row.append(ws.cell(r, c).value)
        rows.append(row)
    return rows


def find_header_index(rows: List[List[Any]]) -> Optional[int]:
    keywords = ("施設", "区", "合計", "0歳", "０歳", "1歳", "１歳", "受入", "待ち", "児童")
    best_i, best_score = None, -1
    for i, row in enumerate(rows[:120]):
        cells = ["" if v is None else str(v) for v in row]
        nonempty = sum(1 for c in cells if c.strip() != "")
        has_kw = any(any(k in c for k in keywords) for c in cells)
        score = nonempty + (10 if has_kw else 0)
        if nonempty >= 5 and score > best_score:
            best_i, best_score = i, score
    return best_i


def parse_sheet(ws) -> Tuple[Optional[str], List[Dict[str, str]]]:
    rows = sheet_to_rows(ws)

    # 月をまず探す（タイトル行やシート名から）
    month = extract_month_from_text(ws.title)
    if month is None:
        for r in rows[:20]:
            for v in r[:10]:
                month = extract_month_from_text("" if v is None else str(v))
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


def read_xlsx(url: str) -> Dict[str, List[Dict[str, str]]]:
    """
    xlsx 1ファイル → {month: rows} を返す（同月が複数シートなら後勝ち）
    """
    print("download:", url)
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    wb = load_workbook(io.BytesIO(r.content), data_only=True)

    mp: Dict[str, List[Dict[str, str]]] = {}
    for ws in wb.worksheets:
        month, rows = parse_sheet(ws)
        if month and rows:
            mp[month] = rows
    print("parsed months:", len(mp))
    return mp


# ---------- column guessing / metrics ----------
def guess_facility_id_key(rows: List[Dict[str, str]]) -> str:
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

    # content-based
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
    # 0-5 internal
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
    print("BACKFILL start. ward=", WARD_FILTER, "months_back=", MONTHS_BACK)

    urls = scrape_excel_urls()
    master = load_master()
    target = norm(WARD_FILTER) if WARD_FILTER else None

    # download + parse all candidate xlsx for each metric
    acc_by_month: Dict[str, List[Dict[str, str]]] = {}
    wai_by_month: Dict[str, List[Dict[str, str]]] = {}
    enr_by_month: Dict[str, List[Dict[str, str]]] = {}

    for u in urls["accept"]:
        try:
            acc_by_month.update(read_xlsx(u))
        except Exception as e:
            print("WARN accept xlsx failed:", u, e)

    for u in urls["wait"]:
        try:
            wai_by_month.update(read_xlsx(u))
        except Exception as e:
            print("WARN wait xlsx failed:", u, e)

    for u in urls["enrolled"]:
        try:
            enr_by_month.update(read_xlsx(u))
        except Exception as e:
            print("WARN enrolled xlsx failed:", u, e)

    if not acc_by_month:
        raise RuntimeError("受入可能数の月次が1つも取れませんでした")

    # target months: last N months (month floors)
    end = month_floor(date.today())
    start = add_months(end, -(MONTHS_BACK - 1))
    want = []
    cur = start
    while cur <= end:
        want.append(iso(cur))
        cur = add_months(cur, 1)

    available = [m for m in want if m in acc_by_month]
    print("want months:", len(want), "available:", len(available), "missing:", [m for m in want if m not in acc_by_month])

    # months.json existing
    months_path = DATA_DIR / "months.json"
    existing_months: List[str] = []
    if months_path.exists():
        try:
            existing_months = json.loads(months_path.read_text(encoding="utf-8")).get("months", [])
        except Exception:
            existing_months = []

    # generate each month
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

        W = {}
        if wait_rows:
            try:
                fid_w = guess_facility_id_key(wait_rows)
                W = index_by_key(wait_rows, fid_w)
            except Exception:
                W = {}

        E = {}
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

            tot_accept = get_total(ar)
            tot_wait = get_total(wr) if wr else None
            tot_enrolled = get_total(er) if er else None
            cap_est = (tot_enrolled + tot_accept) if (tot_enrolled is not None and tot_accept is not None) else None

            age_groups, ages_0_5 = build_age_groups(ar, wr, er)

            facilities.append({
                "id": fid,
                "name": name,
                "ward": ward,
                "address": address,
                "map_url": map_url,
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
            })

        out_path.write_text(
            json.dumps({"month": m, "ward": (WARD_FILTER or "横浜市"), "facilities": facilities},
                       ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        print("wrote:", out_path.name, "facilities:", len(facilities))

    # update months.json
    ms = set(existing_months)
    for m in available:
        p = DATA_DIR / f"{m}.json"
        if p.exists() and p.stat().st_size > 200:
            ms.add(m)
    months_path.write_text(json.dumps({"months": sorted(ms)}, ensure_ascii=False, indent=2), encoding="utf-8")
    print("updated months.json:", len(ms))


if __name__ == "__main__":
    main()
