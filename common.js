async function loadJSON(path){
  const r = await fetch(path, {cache:'no-store'});
  if(!r.ok) throw new Error('Failed to load '+path);
  return await r.json();
}
function fmt(n){
  if(n===null || n===undefined || n==='') return '';
  const x = Number(n);
  if(Number.isNaN(x)) return '';
  return x.toLocaleString('ja-JP');
}
function fmtPct(r){
  if(r===null || r===undefined || r==='') return '';
  const x = Number(r);
  if(!Number.isFinite(x)) return '';
  return (x*100).toFixed(1) + '%';
}
function safeStr(x){ return (x===null||x===undefined)?'':String(x); }
