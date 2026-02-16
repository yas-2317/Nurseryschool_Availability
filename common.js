// common.js

function safeStr(x){
  return (x === null || x === undefined) ? '' : String(x);
}

function fmt(x){
  if(x === null || x === undefined || x === '') return '—';
  const n = Number(x);
  if(Number.isNaN(n)) return safeStr(x);
  return n.toLocaleString('ja-JP');
}

function getParam(key){
  const u = new URL(location.href);
  return u.searchParams.get(key);
}

async function loadJSON(path){
  // path is relative to current page (index.html/facility.html are same dir)
  const url = new URL(path, location.href);
  const r = await fetch(url.toString(), { cache: 'no-store' });
  if(!r.ok){
    throw new Error(`Failed to load ${path}`);
  }
  return await r.json();
}

/* ===========================
   Kana normalization for search
   - Convert Katakana -> Hiragana
   - Normalize spaces
   =========================== */

function kataToHira(s){
  let out = '';
  for(const ch of safeStr(s)){
    const code = ch.charCodeAt(0);
    if(code >= 0x30A1 && code <= 0x30F6){
      out += String.fromCharCode(code - 0x60);
    }else{
      out += ch;
    }
  }
  return out;
}

function normalizeForSearch(s){
  // keep kanji as-is, but normalize kana & spaces
  let x = safeStr(s);
  x = x.replace(/　/g, ' ');
  x = x.replace(/\s+/g, ' ').trim();
  x = kataToHira(x);
  // remove spaces for easier includes matching
  x = x.replace(/\s+/g, '');
  return x.toLowerCase();
}

function pickStationKey(f){
  // prefer kana for sorting
  const sk = safeStr(f.station_kana).trim();
  if(sk) return normalizeForSearch(sk);
  const ns = safeStr(f.nearest_station).replace(/駅/g,'').trim();
  if(ns) return normalizeForSearch(ns);
  return '~~~~'; // nulls go last
}
