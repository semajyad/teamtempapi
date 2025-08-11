from __future__ import annotations
import os
import re
import time
import json
from io import BytesIO
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel

# ---------------- Config ----------------
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
SOURCES_FILE = os.getenv("TEAMTEMP_SOURCES_FILE", "teamtemp_sources.json")
CACHE_TTL = int(os.getenv("CACHE_TTL_SEC", "600"))
DEFAULT_SOURCE = os.getenv("TEAMTEMP_URL", "https://teamtempapp.herokuapp.com/bvc/sDtXkQWe")

# ---------------- Models ----------------
@dataclass
class Record:
    date: str
    team: str
    value: float
    tribe: str

class SourceIn(BaseModel):
    url: str
    tribe: str

# ---------------- App ----------------
app = FastAPI(title="TeamTemp Historical Scraper API", version="4.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_client = httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=30.0, follow_redirects=True)
_cache: Dict[str, object] = {"ts": 0.0, "data": [], "sources": []}

# ---------------- Source persistence ----------------
def _load_sources() -> List[Dict[str, str]]:
    if os.path.exists(SOURCES_FILE):
        try:
            with open(SOURCES_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                out: List[Dict[str, str]] = []
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and "url" in item:
                            out.append({"url": str(item["url"]).strip(), "tribe": str(item.get("tribe", "")).strip()})
                        elif isinstance(item, str):
                            out.append({"url": item.strip(), "tribe": ""})
                return [s for s in out if s.get("url")]
        except Exception:
            pass
    return ([{"url": DEFAULT_SOURCE, "tribe": ""}] if DEFAULT_SOURCE else [])

def _save_sources(sources: List[Dict[str, str]]) -> None:
    try:
        with open(SOURCES_FILE, "w", encoding="utf-8") as f:
            json.dump(sources, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# ---------------- Scraper ----------------
HISTORICAL_RE = re.compile(
    r"var\s+historical_data\s*=\s*new\s+google\.visualization\.DataTable\(\s*(\{.*?\})\s*(?:,\s*[^)]*)?\)\s*;",
    re.IGNORECASE | re.DOTALL,
)
DATE_IN_STRING_RE = re.compile(
    r"^\s*Date\(\s*(\d{4})\s*,\s*(\d{1,2})\s*,\s*(\d{1,2})(?:\s*,\s*(\d{1,2})\s*,\s*(\d{1,2})\s*,\s*(\d{1,2})(?:\s*,\s*(\d{1,3}))?)?\s*\)\s*$"
)

def _fetch_html(url: str) -> str:
    r = _client.get(url)
    r.raise_for_status()
    return r.text

def _extract_payload(html: str) -> Optional[dict]:
    m = HISTORICAL_RE.search(html)
    if not m:
        soup = BeautifulSoup(html, "html.parser")
        scripts_text = "\n".join(s.get_text("\n", strip=False) for s in soup.find_all("script"))
        m = HISTORICAL_RE.search(scripts_text)
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

def _parse_date_cell(v_obj: object) -> Optional[str]:
    if isinstance(v_obj, str):
        mm = DATE_IN_STRING_RE.match(v_obj)
        if mm:
            y = int(mm.group(1)); mo = int(mm.group(2)); d = int(mm.group(3))
            mo += 1
            return f"{y:04d}-{mo:02d}-{d:02d}"
    return None

def _rows_to_records(payload: dict, tribe: str) -> List[Record]:
    cols = payload.get("cols", [])
    rows = payload.get("rows", [])
    if not cols or not rows:
        return []
    labels: List[str] = []
    for i, col in enumerate(cols[1:], start=1):
        label = str(col.get("label") or col.get("id") or f"col{i}")
        labels.append(label)
    if labels and labels[-1].strip().lower() == "average":
        labels = labels[:-1]
    out: List[Record] = []
    for row in rows:
        cells = row.get("c", []) if isinstance(row, dict) else []
        if not cells or len(cells) < 2:
            continue
        date_iso = _parse_date_cell(cells[0].get("v") if isinstance(cells[0], dict) else None) or time.strftime("%Y-%m-%d")
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
                continue
    return out

def scrape_one(url: str, tribe: str) -> List[Record]:
    html = _fetch_html(url)
    payload = _extract_payload(html)
    if not payload:
        return []
    return _rows_to_records(payload, tribe)

# ---------------- Frontend ----------------
INDEX_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>TeamTemp sources</title>
</head>
<body>
  <h1>TeamTemp Sources</h1>
  <input id="url" placeholder="TeamTemp URL" size="50">
  <input id="tribe" placeholder="Tribe">
  <button onclick="addSource()">Add</button>
  <button onclick="refreshData()">Refresh</button>
  <button onclick="downloadExcel()">Download Excel</button>
  <table border="1" cellpadding="5" id="tbl"></table>

<script>
async function loadSources(){
  const res = await fetch('/sources'); const js = await res.json();
  let html = '<tr><th>URL</th><th>Tribe</th></tr>';
  js.sources.forEach(s => html += `<tr><td>${s.url}</td><td>${s.tribe}</td></tr>`);
  document.getElementById('tbl').innerHTML = html;
}
async function addSource(){
  const url = document.getElementById('url').value;
  const tribe = document.getElementById('tribe').value;
  await fetch('/sources', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({url, tribe})});
  loadSources();
}
async function refreshData(){
  await fetch('/data?force=true');
  alert('Data refreshed');
}
function downloadExcel(){
  window.location = '/export.xlsx?force=true';
}
loadSources();
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
def index():
    return HTMLResponse(INDEX_HTML)

# ---------------- CRUD ----------------
@app.get("/sources")
def list_sources():
    sources = _cache.get("sources") or _load_sources()
    _cache["sources"] = sources
    return {"sources": sources}

@app.post("/sources")
def add_source(src: SourceIn):
    url = src.url.strip()
    tribe = src.tribe.strip()
    if not url.startswith("http"):
        raise HTTPException(400, "Invalid URL")
    sources = _cache.get("sources") or _load_sources()
    for s in sources:
        if s.get("url") == url:
            s["tribe"] = tribe
            _save_sources(sources)
            _cache["sources"] = sources
            return {"ok": True}
    sources.append({"url": url, "tribe": tribe})
    _save_sources(sources)
    _cache["sources"] = sources
    return {"ok": True}

# ---------------- Data ----------------
@app.get("/data")
def get_data(force: bool = Query(False)):
    now = time.time()
    if not force and (now - float(_cache.get("ts", 0))) < CACHE_TTL and _cache.get("data"):
        return _cache["data"]
    merged: List[Dict[str, object]] = []
    for s in _load_sources():
        merged.extend([r.__dict__ for r in scrape_one(s["url"], s["tribe"])])
    _cache["ts"] = now
    _cache["data"] = merged
    return merged

@app.get("/export.xlsx")
def export_excel(force: bool = Query(False)):
    if force or not _cache.get("data"):
        get_data(force=True)
    rows = _cache["data"]
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.append(["tribe", "team", "date", "value"])
    for r in rows:
        ws.append([r["tribe"], r["team"], r["date"], r["value"]])
    stream = BytesIO()
    wb.save(stream)
    stream.seek(0)
    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=teamtemp.xlsx"}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("teamtemp_multi_sources_with_tribe:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
