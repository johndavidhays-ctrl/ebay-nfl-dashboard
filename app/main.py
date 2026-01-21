# app/main.py
from typing import List, Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from .db import SessionLocal, ensure_schema, Item


app = FastAPI(title="NFL Card Scanner")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _startup() -> None:
    ensure_schema()


def _get_db() -> Session:
    return SessionLocal()


@app.get("/health")
def health() -> dict:
    return {"ok": True}


HOME_HTML = (
    "<!doctype html>"
    "<html>"
    "<head>"
    '<meta charset="utf-8">'
    "<title>NFL Card Scanner</title>"
    "<style>"
    "body{font-family:Arial,Helvetica,sans-serif;margin:20px}"
    "table{border-collapse:collapse;width:100%}"
    "th,td{border:1px solid #ddd;padding:8px;vertical-align:top}"
    "th{background:#f4f4f4;text-align:left}"
    "img{max-width:90px;height:auto}"
    ".muted{color:#666;font-size:12px}"
    "</style>"
    "</head>"
    "<body>"
    "<h2>Latest results</h2>"
    '<div class="muted">If you see an empty table, run the scanner cron once.</div>'
    '<table id="t">'
    "<thead>"
    "<tr>"
    "<th>Card</th>"
    "<th>Price</th>"
    "<th>Ends</th>"
    "<th>Query</th>"
    "</tr>"
    "</thead>"
    "<tbody></tbody>"
    "</table>"
    "<script>"
    "async function load(){"
    "  const r = await fetch('/items?limit=200');"
    "  const data = await r.json();"
    "  const tb = document.querySelector('#t tbody');"
    "  tb.innerHTML = '';"
    "  for(const it of data.items){"
    "    const tr = document.createElement('tr');"
    "    const img = it.image_url ? `<img src=\"${it.image_url}\">` : '';"
    "    const title = it.url ? `<a href=\"${it.url}\" target=\"_blank\" rel=\"noreferrer\">${it.title || it.ebay_item_id}</a>` : (it.title || it.ebay_item_id);"
    "    tr.innerHTML = `"
    "      <td>${img}<div>${title}</div><div class=\"muted\">${it.ebay_item_id}</div></td>"
    "      <td>${it.currency} ${Number(it.total_price || 0).toFixed(2)}</td>"
    "      <td>${it.end_time || ''}</td>"
    "      <td>${it.query || ''}</td>"
    "    `;"
    "    tb.appendChild(tr);"
    "  }"
    "}"
    "load();"
    "</script>"
    "</body>"
    "</html>"
)


@app.get("/", response_class=HTMLResponse)
def home() -> str:
    return HOME_HTML


@app.get("/items")
def list_items(
    limit: int = Query(100, ge=1, le=500),
    active: bool = Query(True),
    q: Optional[str] = Query(None),
) -> JSONResponse:
    db = _get_db()
    try:
        stmt = select(Item)
        if active:
            stmt = stmt.where(Item.active.is_(True))
        if q:
            stmt = stmt.where(Item.title.ilike(f"%{q}%"))
        stmt = stmt.order_by(desc(Item.updated_at)).limit(limit)
        items: List[Item] = list(db.scalars(stmt).all())
        return JSONResponse({"items": [it.to_dict() for it in items]})
    finally:
        db.close()
