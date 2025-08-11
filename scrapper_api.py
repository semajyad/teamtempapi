from __future__ import annotations
import os, re, time, json, uuid, tempfile
from io import BytesIO
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, HttpUrl

# ---------------- Config
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
SOURCES_FILE = os.getenv("TEAMTEMP_SOURCES_FILE", "teamtemp_sources.json")
CACHE_TTL = int(os.getenv("CACHE_TTL_SEC", "600"))
DEFAULT_SOURCE = os.getenv("TEAMTEMP_URL", "https://teamtempapp.herokuapp.com/bvc/sDtXkQWe")
SOURCES_JSON = os.getenv("SOURCES_JSON", "")
APP_VERSION = "4.3.0"

# ---------------- Models
@dataclass
class Record:
    date: str
    team: str
    value: float
    tribe: str

class SourceIn(BaseModel):
    url: HttpUrl
    tribe: str = ""

class Source(SourceIn):
    id: str
    created_ts: float

# ---------------- App
app = FastAPI(title="TeamTemp Historical Scraper API", version=APP_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

_client = httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=30.0, follow_redirects=True)
_cache: Dict[str, object] = {"ts": 0.0, "data": []}

# ---------------- Persistence helpers
def _ensure_ids(rows: List[dict]) -> List[dict]:
    out = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        url = str(r.get("url", "")).strip()
        if not url:
            continue
        tribe = str(r.get("tribe", "")).strip()
        rid = r.get("id") or uuid.uuid4().hex
        cts = float(r.get("created_ts") or time.time())
        out.append({"id": rid, "url": url, "tribe": tribe, "created_ts": cts})
    return out

def _atomic_write_json(path: str, data: object) -> None:
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=d, encoding="utf-8") as tmp:
        json.dump(data, tmp, ensure_ascii=False, indent=2)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = tmp.name
    os.replace(tmp_path, path)

def _seed_sources_if_needed() -> None:
    if os.path.exists(SOURCES_FILE):
        return
    seed: List[dict] = []
    if SOURCES_JSON:
        try:
            seed = [s for s in json.loads(SOURCES_JSON) if isinstance(s, dict)]
        except Exception:
            seed = []
    if not seed and DEFAULT_SOURCE:
        seed = [{"url": DEFAULT_SOURCE, "tribe": ""}]
    if seed:
        _atomic_write_json(SOURCES_FILE, _ensure_ids(seed))

def _read_sources() -> List[dict]:
    _seed_sources_if_needed()
    if os.path.exists(SOURCES_FILE):
        try:
            with open(SOURCES_FILE, "r", encoding="utf-8") as f:
                rows = json.load(f)
        except Exception:
            rows = []
    else:
        rows = []
    rows = _ensure_ids(rows)
    rows.sort(key=lambda r: (float(r.get("created_ts", 0.0)), r.get("id", "")))
    return rows

def _write_sources(rows: List[dict]) -> List[dict]:
    rows = _ensure_ids(rows)
    rows.sort(key=lambda r: (float(r.get("created_ts", 0.0)), r.get("id", "")))
    _atomic_write_json(SOURCES_FILE, rows)
    return rows

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
def list_sources():
    rows = _read_sources()
    return {"sources": rows}

@app.post("/sources", response_model=Source)
def add_source(src: SourceIn):
    rows = _read_sources()
    row = {"id": uuid.uuid4().hex, "url": str(src.url), "tribe": src.tribe, "created_ts": time.time()}
    rows.append(row)
    rows = _write_sources(rows)
    return row  # return the created row

@app.delete("/sources/{sid}")
def delete_source(sid: str = Path(..., description="Source id")):
    rows = _read_sources()
    new = [s for s in rows if s.get("id") != sid]
    if len(new) == len(rows):
        raise HTTPException(404, "Not found")
    _write_sources(new)
    _cache["data"] = []
    return {"ok": True}

@app.get("/data")
def get_data(force: bool = Query(False)):
    now = time.time()
    if not force and (now - float(_cache.get("ts", 0))) < CACHE_TTL and isinstance(_cache.get("data"), list) and _cache["data"]:
        return _cache["data"]
    merged: List[Dict[str, object]] = []
    errors: List[Tuple[str, str]] = []
    for s in _read_sources():
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
