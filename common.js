// common.js — shared helpers for index.html / facility.html
// - Safe string & number formatters
// - Query param helpers
// - Robust JSON loader (no-store + cache-bust)
// - Kana normalization helpers (hiragana/katakana)
// - Search normalization (for name_kana / station_kana)
// - Station sort helpers (station × walk)
// - Minimal toast helper (optional)

(function () {
  "use strict";

  // ---------- basic ----------
  function safeStr(x) {
    return (x === null || x === undefined) ? "" : String(x);
  }

  function isNullLike(s) {
    const t = safeStr(s).trim();
    return (t === "" || t === "-" || t.toLowerCase() === "null");
  }

  function toNumOrNull(x) {
    if (x === null || x === undefined) return null;
    const s = safeStr(x).trim();
    if (isNullLike(s)) return null;
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  }

  function toIntOrNull(x) {
    const n = toNumOrNull(x);
    if (n === null) return null;
    return Math.trunc(n);
  }

  function fmt(x) {
    const n = toNumOrNull(x);
    if (n === null) return "—";
    return n.toLocaleString("ja-JP");
  }

  // alias
  function fmtNum(x) {
    return fmt(x);
  }

  // ---------- URL params ----------
  function getParam(key) {
    try {
      const u = new URL(window.location.href);
      return u.searchParams.get(key);
    } catch (e) {
      // legacy fallback
      const m = new RegExp("[?&]" + key + "=([^&]+)").exec(window.location.search);
      return m ? decodeURIComponent(m[1]) : null;
    }
  }

  function setParam(key, value) {
    const u = new URL(window.location.href);
    if (value === null || value === undefined || String(value).trim() === "") {
      u.searchParams.delete(key);
    } else {
      u.searchParams.set(key, String(value));
    }
    history.replaceState(null, "", u.toString());
  }

  // ---------- JSON loader ----------
  // GitHub Pages sometimes caches aggressively. Use cache: no-store + bust.
  async function loadJSON(path) {
    const url = path + (path.includes("?") ? "&" : "?") + "v=" + Date.now();
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) throw new Error(`Failed to load ${path} (${res.status})`);
    return await res.json();
  }

  // ---------- Kana utilities ----------
  // Katakana -> Hiragana
  function toHira(s) {
    return safeStr(s).replace(/[\u30a1-\u30f6]/g, ch =>
      String.fromCharCode(ch.charCodeAt(0) - 0x60)
    );
  }

  // Hiragana -> Katakana
  function toKata(s) {
    return safeStr(s).replace(/[\u3041-\u3096]/g, ch =>
      String.fromCharCode(ch.charCodeAt(0) + 0x60)
    );
  }

  // normalize for search:
  // - trim
  // - lower
  // - full-width spaces -> spaces
  // - collapse spaces
  // - katakana -> hiragana (so "シンヨコ" and "しんよこ" match)
  function normalizeForSearch(s) {
    let t = safeStr(s);
    t = t.replace(/　/g, " ");        // full-width space
    t = t.replace(/\s+/g, " ");       // collapse
    t = t.trim().toLowerCase();
    t = toHira(t);                    // unify kana to hiragana
    return t;
  }

  // Build searchable text for a facility
  // (index.html can use this if you want a single unified matching.)
  function buildHaystack(f) {
    const parts = [
      safeStr(f?.name),
      safeStr(f?.name_kana),
      safeStr(f?.nearest_station),
      safeStr(f?.station_kana),
      safeStr(f?.ward),
      safeStr(f?.address),
    ];
    return normalizeForSearch(parts.join(" "));
  }

  // ---------- station sorting helpers ----------
  function stripStationSuffix(s) {
    return safeStr(s).replace(/駅$/g, "").trim();
  }

  // station sort key:
  // prefer station_kana, else nearest_station, normalize to hiragana for stable sorting.
  function pickStationKey(f) {
    const sk = safeStr(f?.station_kana).trim();
    const st = safeStr(f?.nearest_station).trim();
    const base = sk || stripStationSuffix(st);
    return normalizeForSearch(base);
  }

  // walk value for sorting
  function pickWalkMinutes(f) {
    const w = toIntOrNull(f?.walk_minutes);
    return (w === null ? 9999 : w);
  }

  // ---------- toast (optional) ----------
  // simple tiny toast so facility.html can call toast("...") safely
  function toast(msg, ms = 1600) {
    const text = safeStr(msg);
    if (!text) return;

    const el = document.createElement("div");
    el.textContent = text;
    el.style.position = "fixed";
    el.style.left = "50%";
    el.style.bottom = "24px";
    el.style.transform = "translateX(-50%)";
    el.style.padding = "10px 14px";
    el.style.borderRadius = "12px";
    el.style.background = "rgba(0,0,0,.78)";
    el.style.color = "#fff";
    el.style.fontSize = "13px";
    el.style.zIndex = "99999";
    el.style.boxShadow = "0 6px 16px rgba(0,0,0,.25)";
    document.body.appendChild(el);

    setTimeout(() => {
      el.style.transition = "opacity .25s ease";
      el.style.opacity = "0";
      setTimeout(() => el.remove(), 260);
    }, ms);
  }

  // ---------- expose ----------
  window.safeStr = safeStr;
  window.fmt = fmt;
  window.fmtNum = fmtNum;
  window.toIntOrNull = toIntOrNull;
  window.getParam = getParam;
  window.setParam = setParam;
  window.loadJSON = loadJSON;

  window.toHira = toHira;
  window.toKata = toKata;
  window.normalizeForSearch = normalizeForSearch;
  window.buildHaystack = buildHaystack;

  window.pickStationKey = pickStationKey;
  window.pickWalkMinutes = pickWalkMinutes;

  window.toast = toast;
})();
