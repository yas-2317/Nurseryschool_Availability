// common.js — shared helpers for index.html / facility.html

function safeStr(x){
  return (x === null || x === undefined) ? "" : String(x);
}

function fmt(x){
  if (x === null || x === undefined) return "—";
  const s = String(x).trim();
  if (s === "" || s.toLowerCase() === "null" || s === "-") return "—";
  const n = Number(s);
  if (!Number.isFinite(n)) return "—";
  return n.toLocaleString("ja-JP");
}

// alias
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

// JSON loader with no-cache hint
async function loadJSON(path){
  const url = path + (path.includes("?") ? "&" : "?") + "v=" + Date.now();
  const res = await fetch(url, { cache: "no-store" });
  if(!res.ok) throw new Error(`Failed to load ${path} (${res.status})`);
  return await res.json();
}

/* =========================
   Phone helpers
   ========================= */

// "+81 ..." や "81..." を "0..." の国内表記へ寄せる
function normalizeJPPhone(raw){
  let s = safeStr(raw).trim();
  if(!s) return "";

  // keep digits and +
  s = s.replace(/[^\d+]/g, "");

  // "+81" -> "0"
  if(s.startsWith("+81")){
    let rest = s.slice(3);
    if(rest.startsWith("0")) rest = rest.slice(1);
    s = "0" + rest;
  }

  // "81xxxxxxxxx"（+無し）にも一応対応
  if(!s.startsWith("0") && s.startsWith("81")){
    let rest = s.slice(2);
    if(rest.startsWith("0")) rest = rest.slice(1);
    s = "0" + rest;
  }

  // 末尾以外に + が残るのを除去
  s = s.replace(/\+/g, "");
  return s;
}

// できる範囲でハイフン整形（完璧な全国対応はしない）
function formatJPPhone(raw){
  const s = normalizeJPPhone(raw);
  if(!s) return "";

  const digits = s.replace(/[^\d]/g, "");
  const len = digits.length;

  // 携帯: 070/080/090-xxxx-xxxx
  if(len === 11 && /^(070|080|090)/.test(digits)){
    return `${digits.slice(0,3)}-${digits.slice(3,7)}-${digits.slice(7)}`;
  }

  // 東京/大阪: 03/06-xxxx-xxxx
  if(len === 10 && /^(03|06)/.test(digits)){
    return `${digits.slice(0,2)}-${digits.slice(2,6)}-${digits.slice(6)}`;
  }

  // その他(ざっくり): 0xx-xxx-xxxx or 0xxx-xx-xxxx は厳密判定が必要なので、
  // 横浜系(045/044/046等)も含め、まずは 3-3-4 で見やすくする
  if(len === 10){
    return `${digits.slice(0,3)}-${digits.slice(3,6)}-${digits.slice(6)}`;
  }

  // 不明長はそのまま
  return s;
}

function telHref(phone){
  const s = safeStr(phone).trim();
  if(!s) return "#";
  // tel: は数字と + - くらいなら許容されるが、確実に数字のみで
  const digits = s.replace(/[^\d+]/g, "");
  return `tel:${digits}`;
}

// Expose for safety
window.safeStr = safeStr;
window.fmt = fmt;
window.fmtNum = fmtNum;
window.toIntOrNull = toIntOrNull;
window.getParam = getParam;
window.loadJSON = loadJSON;

window.normalizeJPPhone = normalizeJPPhone;
window.formatJPPhone = formatJPPhone;
window.telHref = telHref;
