# teamtemp_multi_sources_with_tribe.py
from __future__ import annotations
import os, re, time, json
from io import BytesIO
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse

from storage_pg import init_and_seed, list_sources, add_source, delete_source

# ---------------- Config
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
CACHE_TTL = int(os.getenv("CACHE_TTL_SEC", "600"))
DEFAULT_SOURCE = os.getenv("TEAMTEMP_URL", "https://teamtempapp.herokuapp.com/bvc/sDtXkQWe")
SOURCES_JSON = os.getenv("SOURCES_JSON", "")
APP_VERSION = "4.3.1"

# initialize DB and seed if empty (idempotent)
init_and_seed(default_source=DEFAULT_SOURCE, sources_json=SOURCES_JSON)

# ---------------- Models
@dataclass
class Record:
    date: str
    team: str
    value: float
    tribe: str

# ---------------- App
app = FastAPI(title="TeamTemp Historical Scraper API", version=APP_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

_client = httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=30.0, follow_redirects=True)
_cache: Dict[str, object] = {"ts": 0.0, "data": []}

# ---------------- Scraper
HISTORICAL_RE = re.compile(
    r"var\s+historical_data\s*=\s*new\s+google\.visualization\.DataTable\(\s*(\{.*?\})\s*(?:,\s*[^)]*)?\)\s*;",
    re.IGNORECASE | re.DOTALL,
)
DATE_IN_STRING_RE = re.compile(r"^\s*Date\((\d{4}),\s*(\d{1,2}),\s*(\d{1,2}).*?\)\s*$")

def _fetch_html(url: str) -> str:
    r = _client.get(url)
    r.raise_for_status()
    return r.text

def _extract_payload(html: str) -> Optional[dict]:
    m = HISTORICAL_RE.search(html)
    if not m:
        soup = BeautifulSoup(html, "html.parser")
        m = HISTORICAL_RE.search("\n".join(s.get_text("\n", strip=False) for s in soup.find_all("script")))
        if not m:
            return None
    obj = m.group(1)
    try:
        return json.loads(obj)
    except json.JSONDecodeError:
        try:
            return json.loads(obj.replace("'", '"'))
        except Exception:
            return None

def _parse_date_cell(v: object) -> Optional[str]:
    if isinstance(v, str):
        mm = DATE_IN_STRING_RE.match(v)
        if mm:
            y, mo, d = int(mm.group(1)), int(mm.group(2)) + 1, int(mm.group(3))
            return f"{y:04d}-{mo:02d}-{d:02d}"
    return None

def _rows_to_records(payload: dict, tribe: str) -> List[Record]:
    cols = payload.get("cols", [])
    rows = payload.get("rows", [])
    if not cols or not rows:
        return []
    labels = [str(c.get("label") or c.get("id") or f"col{i}") for i, c in enumerate(cols[1:], start=1)]
    # last column is "Average" in this dataset
    if labels and labels[-1].strip().lower() == "average":
        labels = labels[:-1]
    out: List[Record] = []
    for row in rows:
        cells = row.get("c", [])
        if not cells:
            continue
        date_iso = _parse_date_cell(cells[0].get("v")) or time.strftime("%Y-%m-%d")
        for j, team in enumerate(labels, start=1):
            if j >= len(cells):
                continue
            cell = cells[j]
            if not isinstance(cell, dict):
                continue
            v = cell.get("v")
            if v is None:
                continue
            try:
                out.append(Record(date=date_iso, team=team, value=float(v), tribe=tribe))
            except Exception:
                pass
    return out

def scrape_one(url: str, tribe: str) -> List[Record]:
    html = _fetch_html(url)
    payload = _extract_payload(html)
    return _rows_to_records(payload, tribe) if payload else []

# ---------------- Frontend
INDEX_HTML = """
<!doctype html><html><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>TeamTemp sources</title>
<style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:24px}
.row{display:flex;gap:8px;margin-bottom:12px}
input{padding:8px} #url{flex:2} #tribe{flex:1}
button{padding:8px 12px;cursor:pointer}
table{border-collapse:collapse;width:100%;margin-top:12px}
th,td{border-bottom:1px solid #eee;text-align:left;padding:8px}
code{font-family:ui-monospace,Menlo,Consolas,monospace}
.badge{font-size:12px;color:#666}
</style></head><body>
<h1>TeamTemp sources</h1>
<div class="row">
  <input id="url" type="url" placeholder="https://teamtempapp.herokuapp.com/bvc/...">
  <input id="tribe" type="text" placeholder="Tribe">
  <button id="add">Add</button>
  <button id="refresh">Refresh data</button>
  <button id="dl">Download Excel</button>
</div>
<div id="msg" class="badge"></div>
<table><thead><tr><th>URL</th><th>Tribe</th><th>Actions</th></tr></thead><tbody id="rows"></tbody></table>
<script>
const el=(id)=>document.getElementById(id);
const api=(p,opt={})=>fetch(p,Object.assign({headers:{'Content-Type':'application/json'}},opt));

async function load(){
  const r = await api('/sources');
  const js = await r.json();
  const tb = el('rows'); tb.innerHTML='';
  (js.sources||[]).forEach(s=>{
    const tr=document.createElement('tr');
    tr.innerHTML = `<td><code>${s.url}</code></td><td>${s.tribe||''}</td>
      <td><button data-id="${s.id}" class="rm">Remove</button></td>`;
    tb.appendChild(tr);
  });
  document.querySelectorAll('.rm').forEach(b=>{
    b.onclick = async ()=>{
      await api('/sources/'+b.dataset.id,{method:'DELETE'});
      load();
    };
  });
}
el('add').onclick = async ()=>{
  const url = el('url').value.trim(), tribe = el('tribe').value.trim();
  if(!url){ el('msg').textContent = 'Enter URL'; return; }
  const r = await api('/sources',{method:'POST',body:JSON.stringify({url,tribe})});
  if(!r.ok){ el('msg').textContent = 'Failed: '+await r.text(); return; }
  el('url').value=''; el('tribe').value='';
  load();
};
el('refresh').onclick = async ()=>{ el('msg').textContent='Refreshing…'; await api('/data?force=true'); el('msg').textContent='Refreshed'; };
el('dl').onclick = ()=>{ window.location.href='/export.xlsx?force=true'; };
load();
</script>
</body></html>
"""

# ---------------- Routes
@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(INDEX_HTML)

@app.get("/version")
def version():
    return {"version": APP_VERSION}

@app.get("/sources")
def sources_list_route():
    return {"sources": list_sources()}

@app.post("/sources")
def sources_add_route(payload: dict):
    url = str(payload.get("url","")).strip()
    tribe = str(payload.get("tribe","")).strip()
    if not url:
        raise HTTPException(400, "url required")
    row = add_source(url, tribe)
    _cache["data"] = []  # invalidate cache
    return row

@app.delete("/sources/{sid}")
def sources_delete_route(sid: str):
    if not delete_source(sid):
        raise HTTPException(404, "Not found")
    _cache["data"] = []
    return {"ok": True}

@app.get("/data")
def get_data(force: bool = Query(False)):
    now = time.time()
    if not force and (now - float(_cache.get("ts", 0))) < CACHE_TTL and isinstance(_cache.get("data"), list) and _cache["data"]:
        return _cache["data"]
    merged: List[Dict[str, object]] = []
    errors: List[Tuple[str, str]] = []
    for s in list_sources():  # <-- from Postgres
        try:
            for rec in scrape_one(s["url"], s.get("tribe", "")):
                merged.append(rec.__dict__)
        except httpx.HTTPError as e:
            errors.append((s["url"], f"http {e}"))
        except Exception as e:
            errors.append((s["url"], str(e)))
    _cache["ts"] = now
    _cache["data"] = merged
    return {"data": merged, "errors": errors} if errors else merged

def _excel_from_rows(rows: List[Dict[str, object]]) -> BytesIO:
    from openpyxl import Workbook
    from openpyxl.utils import get_column_letter
    wb = Workbook(); ws = wb.active; ws.title = "TeamTemp"
    header = ["tribe","team","date","value"]; ws.append(header)
    for r in rows:
        ws.append([r.get("tribe",""), r.get("team",""), r.get("date",""), r.get("value","")])
    for i in range(1, len(header)+1):
        maxlen = max(len(str(ws.cell(row=r, column=i).value or "")) for r in range(1, ws.max_row+1))
        ws.column_dimensions[get_column_letter(i)].width = min(maxlen+2, 60)
    bio = BytesIO(); wb.save(bio); bio.seek(0); return bio

@app.get("/export.xlsx")
def export_excel(force: bool = Query(False)):
    if force or not _cache.get("data"):
        get_data(force=True)
    rows = _cache["data"] if isinstance(_cache["data"], list) else _cache["data"].get("data", [])
    stream = _excel_from_rows(rows)
    fname = f"teamtemp_{time.strftime('%Y-%m-%d')}.xlsx"
    return StreamingResponse(
        stream,
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# ---------------- Main
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("teamtemp_multi_sources_with_tribe:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
