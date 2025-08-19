# storage_pg.py
from __future__ import annotations
import os, time, uuid, json
from typing import List, Dict
from sqlalchemy import create_engine, text

# Heroku’s DATABASE_URL might be postgres:// -> normalize to postgresql://
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL not set")
if DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

def init_and_seed(default_source: str = "", sources_json: str = "") -> None:
    with engine.begin() as conn:
        conn.execute(text("""
        create table if not exists sources (
            id text primary key,
            url text not null,
            tribe text default '',
            created_ts double precision not null
        )
        """))
        # seed once if empty
        count = conn.execute(text("select count(*) from sources")).scalar_one()
        if count == 0:
            seed: List[Dict[str, str]] = []
            if sources_json:
                try:
                    seed = [r for r in json.loads(sources_json) if isinstance(r, dict)]
                except Exception:
                    seed = []
            if not seed and default_source:
                seed = [{"url": default_source, "tribe": ""}]
            now = time.time()
            for r in seed:
                conn.execute(text("""
                insert into sources (id,url,tribe,created_ts)
                values (:id,:url,:tribe,:ts)
                """), {"id": uuid.uuid4().hex, "url": r.get("url","").strip(),
                       "tribe": (r.get("tribe") or "").strip(), "ts": now})

def list_sources() -> List[dict]:
    with engine.begin() as conn:
        rs = conn.execute(text("select id,url,tribe,created_ts from sources order by created_ts,id"))
        return [dict(r._mapping) for r in rs]

def add_source(url: str, tribe: str) -> dict:
    row = {"id": uuid.uuid4().hex, "url": url.strip(), "tribe": tribe.strip(), "created_ts": time.time()}
    with engine.begin() as conn:
        conn.execute(text("""
        insert into sources (id,url,tribe,created_ts)
        values (:id,:url,:tribe,:ts)
        """), {"id": row["id"], "url": row["url"], "tribe": row["tribe"], "ts": row["created_ts"]})
    return row

def delete_source(sid: str) -> bool:
    with engine.begin() as conn:
        res = conn.execute(text("delete from sources where id=:id"), {"id": sid})
        return res.rowcount > 0
