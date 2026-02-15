#!/usr/bin/env python3
"""
横浜市オープンデータ（保育所等の入所状況）から月次データを取得し、
data/YYYY-MM-01.json と data/months.json を更新します。

※ 港北区だけ抽出する場合は、CSVの「施設所在区」が港北区の行だけ残します。
"""
from __future__ import annotations
import csv, json, re
from datetime import date
from pathlib import Path
from typing import Dict, Any, List, Optional
import requests
from bs4 import BeautifulSoup

DATASET_PAGE = "https://data.city.yokohama.lg.jp/dataset/kodomo_nyusho-jokyo"

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

WARD_FILTER = "港北区"  # 必要ならここを変更（全区にしたいなら None にしてもOK）

def to_int(x: Any) -> Optional[int]:
    if x is None: return None
    s = str(x).strip()
    if s == "" or s.lower() == "nan": return None
    try:
        return int(float(s))
    except:
        return None

def detect_month(rows: List[Dict[str,str]]) -> str:
    for k in ("更新日","更新年月日","更新日時"):
        if rows and k in rows[0]:
            return str(rows[0].get(k,"")).strip()[:10]
    today = date.today()
    return date(today.year, today.month, 1).isoformat()

def scrape_csv_urls() -> Dict[str,str]:
    html = requests.get(DATASET_PAGE, timeout=30).text
    soup = BeautifulSoup(html, "html.parser")
    links = [a.get("href","") for a in soup.select("a") if a.get("href","").endswith(".csv")]
    if not links:
        links = re.findall(r"https?://[^\s\"']+\.csv", html)

    best = {}
    # 横浜市オープンデータはファイル名にIDっぽい番号が入ることが多いです（仕様変更時はここを調整）
    for url in links:
        if "0926_" in url: best["accept"] = url       # 受入可能（入所可能人数）
        elif "0929_" in url: best["wait"] = url       # 入所待ち
        elif "0923_" in url: best["enrolled"] = url   # 入所児童数
    if "accept" not in best or "wait" not in best:
        raise RuntimeError("CSVリンク抽出に失敗（ページ仕様変更の可能性）")
    return best

def read_csv(url: str) -> List[Dict[str,str]]:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    for enc in ("cp932","shift_jis","utf-8-sig","utf-8"):
        try:
            text = r.content.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    return list(csv.DictReader(text.splitlines()))

def idx(rows: List[Dict[str,str]]) -> Dict[str, Dict[str,str]]:
    m={}
    for r in rows:
        fid = (r.get("施設番号") or r.get("施設・事業所番号") or "").strip()
        if fid:
            m[fid]=r
    return m

def main():
    urls = scrape_csv_urls()
    accept_rows = read_csv(urls["accept"])
    wait_rows   = read_csv(urls["wait"])
    enrolled_rows = read_csv(urls["enrolled"]) if "enrolled" in urls else []

    month = detect_month(accept_rows)

    A, W, E = idx(accept_rows), idx(wait_rows), idx(enrolled_rows)

    facilities=[]
    for fid, ar in A.items():
        ward = (ar.get("施設所在区") or "").strip()
        if WARD_FILTER and ward != WARD_FILTER:
            continue

        wr = W.get(fid, {})
        er = E.get(fid, {})

        name = (ar.get("施設・事業名") or ar.get("施設名") or "").strip()

        # 住所がまだ無い場合は検索リンク（後で master_facilities.csv を作るなら差し替え可能）
        q = " ".join([name, ward, "横浜市"]).strip()
        map_url = f"https://www.google.com/maps/search/?api=1&query={q}"

        tot_accept = to_int(ar.get("合計") or ar.get("合計_受入可能") or ar.get("入所可能人数（合計）") or ar.get("入所可能人数"))
        tot_wait   = to_int(wr.get("合計") or wr.get("合計_入所待ち") or wr.get("入所待ち人数（合計）") or wr.get("入所待ち人数"))
        tot_enr    = to_int(er.get("合計") or er.get("合計_入所児童") or er.get("入所児童数（合計）") or er.get("入所児童数"))

        tot_cap = (tot_enr + tot_accept) if (tot_enr is not None and tot_accept is not None) else None
        tot_ratio = (tot_wait / tot_cap) if (tot_wait is not None and tot_cap) else None

        ages={}
        for i in range(6):
            # 0〜5歳。列名はCSVで揺れることがあるので、ここは必要に応じて調整
            a = to_int(ar.get(f"{i}歳児") or ar.get(f"{'０１２３４５'[i]}歳児"))
            w = to_int(wr.get(f"{i}歳児") or wr.get(f"{'０１２３４５'[i]}歳児"))
            e = to_int(er.get(f"{i}歳児") or er.get(f"{'０１２３４５'[i]}歳児"))
            cap = (e + a) if (e is not None and a is not None) else None
            ratio = (w / cap) if (w is not None and cap) else None
            ages[str(i)]={"accept":a,"wait":w,"enrolled":e,"capacity":cap,"wait_per_capacity":ratio}

        facilities.append({
            "id": fid,
            "name": name,
            "ward": ward,
            "address": "",
            "map_url": map_url,
            "updated": month,
            "totals": {"accept": tot_accept, "wait": tot_wait, "enrolled": tot_enr, "capacity": tot_cap, "wait_per_capacity": tot_ratio},
            "ages": ages
        })

    (DATA_DIR / f"{month}.json").write_text(
        json.dumps({"month": month, "ward": (WARD_FILTER or "横浜市"), "facilities": facilities}, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    months_path = DATA_DIR / "months.json"
    months = {"months":[month]}
    if months_path.exists():
        old = json.loads(months_path.read_text(encoding="utf-8"))
        ms = set(old.get("months",[]))
        ms.add(month)
        months["months"]=sorted(ms)
    months_path.write_text(json.dumps(months, ensure_ascii=False, indent=2), encoding="utf-8")

    print("OK:", month, "facilities:", len(facilities))

if __name__ == "__main__":
    main()
