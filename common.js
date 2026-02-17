// common.js — shared helpers for index.html / facility.html

function safeStr(x){
  return (x === null || x === undefined) ? "" : String(x);
}

function toHira(s){
  // カタカナ→ひらがな
  return safeStr(s).replace(/[\u30a1-\u30f6]/g, ch =>
    String.fromCharCode(ch.charCodeAt(0) - 0x60)
  );
}

function normalizeForSearch(s){
  // 検索用正規化：
  // - 全角/半角スペース除去
  // - 小文字化
  // - カタカナ→ひらがな
  // - 記号ゆれ軽減
  const x = toHira(safeStr(s))
    .toLowerCase()
    .replace(/[　\s]+/g, "")
    .replace(/[‐-‒–—―ーｰ]/g, "ー")
    .replace(/[（）\(\)\[\]【】「」『』]/g, "");
  return x;
}

function fmt(x){
  if (x === null || x === undefined) return "—";
  const s = String(x).trim();
  if (s === "" || s.toLowerCase() === "null" || s === "-") return "—";
  const n = Number(s);
  if (!Number.isFinite(n)) return "—";
  return n.toLocaleString("ja-JP");
}

function fmtNum(x){
  return fmt(x);
}

function toIntOrNull(x){
  if (x === null || x === undefined) return null;
  const s = String(x).trim();
  if (s === "" || s.toLowerCase() === "null" || s === "-") return null;
  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function getParam(key){
  try{
    const u = new URL(window.location.href);
    return u.searchParams.get(key);
  }catch(e){
    const m = new RegExp("[?&]"+key+"=([^&]+)").exec(window.location.search);
    return m ? decodeURIComponent(m[1]) : null;
  }
}

function setParam(key, value){
  try{
    const u = new URL(window.location.href);
    if(value === null || value === undefined || String(value).trim() === ""){
      u.searchParams.delete(key);
    }else{
      u.searchParams.set(key, String(value));
    }
    history.replaceState(null, "", u.toString());
  }catch(e){
    // ignore
  }
}

function toast(msg, ms=1600){
  let el = document.getElementById("__toast");
  if(!el){
    el = document.createElement("div");
    el.id = "__toast";
    el.style.position = "fixed";
    el.style.left = "50%";
    el.style.bottom = "18px";
    el.style.transform = "translateX(-50%)";
    el.style.padding = "10px 14px";
    el.style.borderRadius = "999px";
    el.style.background = "rgba(0,0,0,.78)";
    el.style.color = "#fff";
    el.style.fontSize = "13px";
    el.style.zIndex = "99999";
    el.style.maxWidth = "92vw";
    el.style.whiteSpace = "nowrap";
    el.style.overflow = "hidden";
    el.style.textOverflow = "ellipsis";
    el.style.display = "none";
    document.body.appendChild(el);
  }
  el.textContent = msg;
  el.style.display = "block";
  clearTimeout(el.__t);
  el.__t = setTimeout(()=>{ el.style.display = "none"; }, ms);
}

// JSON loader with no-cache hint (GitHub Pages cache対策)
async function loadJSON(path){
  const url = path + (path.includes("?") ? "&" : "?") + "v=" + Date.now();
  const res = await fetch(url, { cache: "no-store" });
  if(!res.ok) throw new Error(`Failed to load ${path} (${res.status})`);
  return await res.json();
}

// expose
window.safeStr = safeStr;
window.toHira = toHira;
window.normalizeForSearch = normalizeForSearch;
window.fmt = fmt;
window.fmtNum = fmtNum;
window.toIntOrNull = toIntOrNull;
window.getParam = getParam;
window.setParam = setParam;
window.toast = toast;
window.loadJSON = loadJSON;
