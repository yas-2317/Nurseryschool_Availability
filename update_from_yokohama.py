#!/usr/bin/env python3
"""
横浜市オープンデータ（保育所等の入所状況）から月次データを取得し、
data/YYYY-MM-01.json を生成して履歴を蓄積します。

このスクリプトはネット接続できる環境で実行してください。
"""
from __future__ import annotations
import csv
import json
import re
from datetime import date
from pathlib import Path
from typing import Dict, Any, List, Optional
import requests
from bs4 import BeautifulSoup

DATASET_PAGE = "https://data.city.yokohama.lg.jp/dataset/kodomo_nyusho-jokyo"

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MASTER_CSV = DATA_DIR / "master_facilities.csv"

def load_master() -> Dict[str, Dict[str, str]]:
    if not MASTER_CSV.exists():
        return {}
    out = {}
    with MASTER_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            fid = (row.get("facility_id") or "").strip()
            if fid:
                out[fid]=row
    return out

def scrape_latest_csv_urls() -> Dict[str,str]:
    html = requests.get(DATASET_PAGE, timeout=30).text
    soup = BeautifulSoup(html, "html.parser")
    links = [a.get("href","") for a in soup.select("a") if a.get("href","").endswith(".csv")]
    if not links:
        links = re.findall(r"https?://[^\s\"']+\.csv", html)

    # Yokohama pattern: 0926=受入可能, 0923=入所児童数, 0929=入所待ち
    best={}
    for url in links:
        if "0926_" in url: best["accept"]=url
        elif "0923_" in url: best["enrolled"]=url
        elif "0929_" in url: best["wait"]=url

    if "accept" not in best or "wait" not in best:
        raise RuntimeError("CSVリンクの抽出に失敗しました（DATASET_PAGEのHTML仕様変更の可能性）。")
    return best

def read_csv(url:str) -> List[Dict[str,str]]:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    for enc in ("cp932","shift_jis","utf-8-sig","utf-8"):
        try:
            text = r.content.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = r.text
    return list(csv.DictReader(text.splitlines()))

def to_int(x: Any) -> Optional[int]:
    if x is None: return None
    s = str(x).strip()
    if s=="" or s.lower()=="nan": return None
    try:
        return int(float(s))
    except:
        return None

def detect_month(rows: List[Dict[str,str]]) -> str:
    for k in ("更新日","更新年月日","更新日時"):
        if rows and k in rows[0]:
            return str(rows[0].get(k,"")).strip()[:10]
    today=date.today()
    return date(today.year,today.month,1).isoformat()

def idx(rows: List[Dict[str,str]]) -> Dict[str, Dict[str,str]]:
    m={}
    for r in rows:
        fid = (r.get("施設番号") or r.get("施設・事業所番号") or "").strip()
        if fid:
            m[fid]=r
    return m

def main():
    urls = scrape_latest_csv_urls()
    accept_rows = read_csv(urls["accept"])
    wait_rows = read_csv(urls["wait"])
    month = detect_month(accept_rows)

    enrolled_rows = []
    if "enrolled" in urls:
        try:
            enrolled_rows = read_csv(urls["enrolled"])
        except Exception:
            enrolled_rows = []

    A=idx(accept_rows)
    W=idx(wait_rows)
    E=idx(enrolled_rows) if enrolled_rows else {}

    master=load_master()

    facilities=[]
    for fid, ar in A.items():
        wr = W.get(fid, {})
        er = E.get(fid, {})

        name = (ar.get("施設・事業名") or ar.get("施設名") or "").strip()
        ward = (ar.get("施設所在区") or "").strip()

        m = master.get(fid, {})
        address = (m.get("address") or "").strip()
        if m.get("lat") and m.get("lng"):
            map_url = f"https://www.google.com/maps/search/?api=1&query={m['lat']},{m['lng']}"
        else:
            q = " ".join([name, ward, "横浜市"]).strip()
            map_url = f"https://www.google.com/maps/search/?api=1&query={q}"

        tot_accept=to_int(ar.get("合計") or ar.get("合計_受入可能") or ar.get("入所可能人数（合計）") or ar.get("入所可能人数"))
        tot_wait=to_int(wr.get("合計") or wr.get("合計_入所待ち") or wr.get("入所待ち人数（合計）") or wr.get("入所待ち人数"))
        tot_enr=to_int(er.get("合計") or er.get("合計_入所児童") or er.get("入所児童数（合計）") or er.get("入所児童数"))
        tot_cap = (tot_enr + tot_accept) if (tot_enr is not None and tot_accept is not None) else None
        tot_ratio = (tot_wait / tot_cap) if (tot_wait is not None and tot_cap) else None

        ages={}
        for i in range(6):
            # columns may vary; try both half/full width
            a = to_int(ar.get(f"{i}歳児") or ar.get(f"{'０１２３４５'[i]}歳児") or ar.get(f"{'０１２３４５'[i]}歳児_受入可能"))
            w = to_int(wr.get(f"{i}歳児") or wr.get(f"{'０１２３４５'[i]}歳児") or wr.get(f"{'０１２３４５'[i]}歳児_入所待ち"))
            e = to_int(er.get(f"{i}歳児") or er.get(f"{'０１２３４５'[i]}歳児"))
            cap = (e + a) if (e is not None and a is not None) else None
            ratio = (w / cap) if (w is not None and cap) else None
            ages[str(i)]={"accept":a,"wait":w,"enrolled":e,"capacity":cap,"wait_per_capacity":ratio}

        facilities.append({
            "id": fid,
            "name": name,
            "ward": ward,
            "address": address,
            "map_url": map_url,
            "updated": month,
            "totals": {"accept": tot_accept, "wait": tot_wait, "enrolled": tot_enr, "capacity": tot_cap, "wait_per_capacity": tot_ratio},
            "ages": ages,
            "meta": {
                "facility_type": (m.get("facility_type") or "").strip(),
                "phone": (m.get("phone") or "").strip(),
                "website": (m.get("website") or "").strip(),
                "notes": (m.get("notes") or "").strip(),
            }
        })

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / f"{month}.json").write_text(json.dumps({"month": month, "ward": "港北区", "facilities": facilities}, ensure_ascii=False, indent=2), encoding="utf-8")

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
