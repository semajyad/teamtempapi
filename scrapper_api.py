from __future__ import annotations
import os
import re
import time
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

# =========================
# Config
# =========================
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
SOURCES_FILE = os.getenv("TEAMTEMP_SOURCES_FILE", "teamtemp_sources.json")
CACHE_TTL = int(os.getenv("CACHE_TTL_SEC", "600"))
DEFAULT_SOURCE = os.getenv("TEAMTEMP_URL", "https://teamtempapp.herokuapp.com/bvc/sDtXkQWe")

# =========================
# Models
# =========================
@dataclass
class Record:
    date: str
    team: str
    value: float
    tribe: str

class SourceIn(BaseModel):
    url: str
    tribe: str

# =========================
# App
# =========================
app = FastAPI(title="TeamTemp Historical Scraper API", version="3.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_client = httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=30.0, follow_redirects=True)
_cache: Dict[str, object] = {"ts": 0.0, "data": [], "sources": []}

# =========================
# Sources persistence
# =========================

def _load_sources() -> List[Dict[str, str]]:
    if os.path.exists(SOURCES_FILE):
        try:
            with open(SOURCES_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    norm: List[Dict[str, str]] = []
                    for item in data:
                        if isinstance(item, dict) and "url" in item:
                            norm.append({"url": str(item["url"]).strip(), "tribe": str(item.get("tribe", "")).strip()})
                        elif isinstance(item, str):
                            norm.append({"url": item.strip(), "tribe": ""})
                    return [s for s in norm if s.get("url")]
        except Exception:
            pass
    return ([{"url": DEFAULT_SOURCE, "tribe": ""}] if DEFAULT_SOURCE else [])


def _save_sources(sources: List[Dict[str, str]]) -> None:
    try:
        with open(SOURCES_FILE, "w", encoding="utf-8") as f:
            json.dump(sources, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# =========================
# Scraper for one TeamTemp page
# =========================
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
            mo += 1  # JS month is zero-based
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

# =========================
# Minimal frontend to manage sources
# =========================
INDEX_HTML = """
<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>TeamTemp Sources</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }
    h1 { font-size: 20px; margin-bottom: 12px; }
    .row { display: flex; gap: 8px; margin-bottom: 12px; }
    input[type=url], input[type=text] { padding: 8px; }
    input#url { flex: 2; }
    input#tribe { flex: 1; }
    button { padding: 8px 12px; cursor: pointer; }
    table { border-collapse: collapse; width: 100%; margin-top: 12px; }
    th, td { border-bottom: 1px solid #eee; text-align: left; padding: 8px; }
    code { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    .muted { color: #666; font-size: 12px; }
    .ok { color: #076b34; }
    .err { color: #9b1c1c; }
    .count { font-weight: 600; }
  </style>
</head>
<body>
  <h1>TeamTemp sources</h1>
  <div class=\"row\">
    <input id=\"url\" type=\"url\" placeholder=\"https://teamtempapp.herokuapp.com/bvc/...\" />
    <input id=\"tribe\" type=\"text\" placeholder=\"Tribe name\" />
    <button id=\"add\">Add</button>
    <button id=\"refresh\">Refresh data\n</button>
  </div>
  <div id=\"msg\" class=\"muted\"></div>
  <table>
    <thead><tr><th>URL</th><th>Tribe</th><th></th></tr></thead>
    <tbody id=\"rows\"></tbody>
  </table>

  <script>
    const api = (path, opts={}) => fetch(path, Object.assign({headers:{'Content-Type':'application/json'}}, opts));

    async function loadSources(){
      const r = await api('/sources');
      const data = await r.json();
      const tbody = document.getElementById('rows');
      tbody.innerHTML = '';
      (data.sources||[]).forEach((s) => {
        const tr = document.createElement('tr');
        const tdUrl = document.createElement('td'); tdUrl.innerHTML = `<code>${s.url}</code>`;
        const tdTribe = document.createElement('td'); tdTribe.textContent = s.tribe || '';
        const tdAct = document.createElement('td');
        const btn = document.createElement('button'); btn.textContent = 'Remove';
        btn.onclick = async () => { await api('/sources', {method:'DELETE', body: JSON.stringify({url:s.url})}); loadSources(); };
        tdAct.appendChild(btn);
        tr.appendChild(tdUrl); tr.appendChild(tdTribe); tr.appendChild(tdAct);
        tbody.appendChild(tr);
      });
    }

    document.getElementById('add').onclick = async () => {
      const url = document.getElementById('url').value.trim();
      const tribe = document.getElementById('tribe').value.trim();
      if(!url){ return; }
      const r = await api('/sources', {method:'POST', body: JSON.stringify({url, tribe})});
      if(r.ok){ document.getElementById('url').value=''; document.getElementById('tribe').value=''; loadSources(); setMsg('Added', true); }
      else { const t = await r.text(); setMsg('Add failed: '+t, false); }
    };

    document.getElementById('refresh').onclick = async () => {
      setMsg('Refreshing…');
      const r = await api('/data?force=true');
      const data = await r.json();
      const count = Array.isArray(data) ? data.length : (data.data || []).length;
      setMsg('Refreshed. Records: <span class="count">'+count+'</span>', true);
    };

    function setMsg(text, ok){ const el = document.getElementById('msg'); el.innerHTML = text; el.className = ok? 'ok' : 'err'; }

    loadSources();
  </script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
def index():
    return HTMLResponse(INDEX_HTML)

# =========================
# Routes: sources CRUD
# =========================
@app.get("/sources")
def list_sources():
    sources = _cache.get("sources") or _load_sources()
    _cache["sources"] = sources
    return {"sources": sources}

@app.post("/sources")
def add_source(src: SourceIn):
    url = src.url.strip()
    tribe = src.tribe.strip()
    if not url.startswith("http://") and not url.startswith("https://"):
        raise HTTPException(status_code=400, detail="url must start with http:// or https://")
    sources = _cache.get("sources") or _load_sources()
    if any(s.get("url") == url for s in sources):
        for s in sources:
            if s.get("url") == url:
                s["tribe"] = tribe
        _cache["sources"] = sources
        _save_sources(sources)
        return {"ok": True, "sources": sources}
    sources.append({"url": url, "tribe": tribe})
    _cache["sources"] = sources
    _save_sources(sources)
    return {"ok": True, "sources": sources}

@app.delete("/sources")
def remove_source(src: SourceIn = Body(...)):
    url = src.url.strip()
    sources = _cache.get("sources") or _load_sources()
    if any(s.get("url") == url for s in sources):
        sources = [s for s in sources if s.get("url") != url]
        _cache["sources"] = sources
        _save_sources(sources)
    return {"ok": True, "sources": sources}

# =========================
# Data endpoints
# =========================
@app.get("/ping")
def ping():
    sources = _cache.get("sources") or _load_sources()
    return {"ok": True, "sources_count": len(sources)}

@app.get("/data")
def get_data(force: bool = Query(False, description="Force scrape all sources")):
    now = time.time()
    if not force and (now - float(_cache.get("ts", 0.0))) < CACHE_TTL and _cache.get("data"):
        return _cache["data"]

    sources = _cache.get("sources") or _load_sources()
    merged: List[Dict[str, object]] = []
    errors: List[Tuple[str, str]] = []

    for s in sources:
        url = s.get("url", "").strip()
        tribe = s.get("tribe", "").strip()
        if not url:
            continue
        try:
            recs = scrape_one(url, tribe)
            merged.extend([r.__dict__ for r in recs])
        except httpx.HTTPError as e:
            errors.append((url, f"HTTP {e}"))
        except Exception as e:
            errors.append((url, str(e)))

    _cache["ts"] = now
    _cache["data"] = merged

    if errors:
        return {"data": merged, "errors": errors}
    return merged

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
