# app/main.py
from typing import List, Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select, asc, desc
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
    ".pos{font-weight:700}"
    ".neg{font-weight:700}"
    "</style>"
    "</head>"
    "<body>"
    "<h2>Auctions ending soon</h2>"
    '<div class="muted">Sorted by nearest end time. Market value is estimated from similar fixed price listings.</div>'
    '<table id="t">'
    "<thead>"
    "<tr>"
    "<th>Card</th>"
    "<th>Total</th>"
    "<th>Market</th>"
    "<th>Profit</th>"
    "<th>Ends</th>"
    "<th>Query</th>"
    "</tr>"
    "</thead>"
    "<tbody></tbody>"
    "</table>"
    "<script>"
    "function money(n){ return Number(n || 0).toFixed(2); }"
    "async function load(){"
    "  const r = await fetch('/items?limit=200&active=true&sort=end');"
    "  const data = await r.json();"
    "  const tb = document.querySelector('#t tbody');"
    "  tb.innerHTML = '';"
    "  for(const it of data.items){"
    "    const tr = document.createElement('tr');"
    "    const img = it.image_url ? `<img src=\"${it.image_url}\">` : '';"
    "    const title = it.url ? `<a href=\"${it.url}\" target=\"_blank\" rel=\"noreferrer\">${it.title || it.ebay_item_id}</a>` : (it.title || it.ebay_item_id);"
    "    const profit = Number(it.profit || 0);"
    "    const profitClass = profit >= 0 ? 'pos' : 'neg';"
    "    tr.innerHTML = `"
    "      <td>${img}<div>${title}</div><div class=\"muted\">${it.ebay_item_id}</div></td>"
    "      <td>${it.currency} ${money(it.total_price)}</td>"
    "      <td>${it.currency} ${money(it.market_value)}</td>"
    "      <td class=\"${profitClass}\">${it.currency} ${money(it.profit)}</td>"
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
    sort: str = Query("end"),
) -> JSONResponse:
    db = _get_db()
    try:
        stmt = select(Item)
        if active:
            stmt = stmt.where(Item.active.is_(True))
        if q:
            stmt = stmt.where(Item.title.ilike(f"%{q}%"))

        if sort == "end":
            stmt = stmt.order_by(asc(Item.end_time).nullslast(), desc(Item.updated_at))
        elif sort == "profit":
            stmt = stmt.order_by(desc(Item.profit), asc(Item.end_time).nullslast())
        else:
            stmt = stmt.order_by(desc(Item.updated_at))

        stmt = stmt.limit(limit)
        items: List[Item] = list(db.scalars(stmt).all())
        return JSONResponse({"items": [it.to_dict() for it in items]})
    finally:
        db.close()
