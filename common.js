// common.js

function safeStr(x){
  return (x==null) ? "" : String(x);
}

function fmt(x){
  if(x==null) return "—";
  const n = Number(x);
  if(!isFinite(n)) return "—";
  return n.toLocaleString('ja-JP');
}

function fmtNum(n){
  const x = Number(n);
  if(!isFinite(x)) return "—";
  return x.toLocaleString('ja-JP');
}

function toIntOrNull(x){
  if(x==null) return null;
  const s = String(x).trim();
  if(!s || s.toLowerCase()==='null' || s==='-') return null;
  const n = Number(s);
  if(!isFinite(n)) return null;
  return Math.trunc(n);
}

async function loadJSON(path){
  const r = await fetch(path, {cache: "no-store"});
  if(!r.ok) throw new Error(`Failed to load ${path}`);
  return await r.json();
}

// ---- kana helpers ----
function toHira(s){
  // カタカナ→ひらがな
  return (s||"").replace(/[\u30a1-\u30f6]/g, ch => String.fromCharCode(ch.charCodeAt(0) - 0x60));
}
function toKata(s){
  // ひらがな→カタカナ
  return (s||"").replace(/[\u3041-\u3096]/g, ch => String.fromCharCode(ch.charCodeAt(0) + 0x60));
}

// normalize for search (lowercase, trim, remove spaces, normalize kana)
function normalizeForSearch(s){
  let x = safeStr(s).toLowerCase();
  x = x.replace(/\s+/g,'').trim();
  // unify long vowel / small kana variations lightly
  x = x.replace(/ー/g,'');
  // keep original; caller may pass hira/kata conversions too
  return x;
}

// ---- URL params ----
function getParam(key){
  try{
    const u = new URL(location.href);
    return u.searchParams.get(key);
  }catch(e){
    return null;
  }
}

function setParam(key, value){
  const v = safeStr(value).trim();
  const u = new URL(location.href);
  if(!v){
    u.searchParams.delete(key);
  } else {
    u.searchParams.set(key, v);
  }
  history.replaceState(null, "", u.toString());
}

// ---- toast ----
function toast(msg){
  const t = document.createElement('div');
  t.className = 'toast';
  t.textContent = msg;
  document.body.appendChild(t);
  setTimeout(()=>{ t.style.opacity = '0.0'; t.style.transition = 'opacity .2s'; }, 1200);
  setTimeout(()=>{ t.remove(); }, 1500);
}
