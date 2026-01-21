# app/main.py
from datetime import datetime, timezone

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import select, asc

from .db import SessionLocal, ensure_schema, Item

app = FastAPI(title="NFL Card Scanner")


@app.on_event("startup")
def startup() -> None:
    ensure_schema()


@app.get("/health")
def health() -> dict:
    return {"ok": True}


def minutes_away(end_iso: str) -> int:
    try:
        end = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        return int((end - now).total_seconds() // 60)
    except Exception:
        return 10**9


@app.get("/items")
def items(
    limit: int = Query(200, ge=1, le=500),
    min_profit: float = Query(75.0),
) -> JSONResponse:
    now = datetime.now(timezone.utc)
    db = SessionLocal()
    try:
        rows = db.scalars(
            select(Item)
            .where(Item.active.is_(True))
            .where(Item.profit >= min_profit)
            .order_by(asc(Item.end_time).nullslast())
            .limit(limit)
        ).all()

        out = []
        for it in rows:
            mins = None
            if it.end_time:
                mins = int((it.end_time - now).total_seconds() // 60)
            out.append({**it.to_dict(), "ends_in_minutes": mins})

        return JSONResponse({"items": out})
    finally:
        db.close()


HOME_HTML = (
    "<!doctype html>"
    "<html><head><meta charset='utf-8'>"
    "<title>Auctions ending soon</title>"
    "<style>"
    "body{font-family:Arial,Helvetica,sans-serif;margin:18px}"
    "table{border-collapse:collapse;width:100%}"
    "th,td{border:1px solid #ddd;padding:8px;vertical-align:top}"
    "th{background:#f4f4f4;text-align:left}"
    "img{max-width:90px;height:auto}"
    ".muted{color:#666;font-size:12px}"
    "</style>"
    "</head><body>"
    "<h2>Auctions ending soon</h2>"
    "<div class='muted'>Only shows items with profit at least $75, sorted by ends soonest.</div>"
    "<table id='t'>"
    "<thead><tr>"
    "<th>Card</th>"
    "<th>Total</th>"
    "<th>Market</th>"
    "<th>Profit</th>"
    "<th>Ends in</th>"
    "<th>Query</th>"
    "</tr></thead>"
    "<tbody></tbody>"
    "</table>"
    "<script>"
    "function money(n){return Number(n||0).toFixed(2)}"
    "function fmtMins(m){"
    "  if(m===null||m===undefined) return '';"
    "  if(m<0) return 'ended';"
    "  if(m<60) return m+' min';"
    "  const h=Math.floor(m/60);"
    "  const mm=m%60;"
    "  return h+' hr '+mm+' min';"
    "}"
    "async function load(){"
    "  const r=await fetch('/items?limit=300&min_profit=75');"
    "  const data=await r.json();"
    "  const tb=document.querySelector('#t tbody');"
    "  tb.innerHTML='';"
    "  for(const it of data.items){"
    "    const img=it.image_url?`<img src=\"${it.image_url}\">`:'';"
    "    const title=it.url?`<a href=\"${it.url}\" target=\"_blank\" rel=\"noreferrer\">${it.title||it.ebay_item_id}</a>`:(it.title||it.ebay_item_id);"
    "    const mins=fmtMins(it.ends_in_minutes);"
    "    tb.innerHTML+=`"
    "      <tr>"
    "        <td>${img}<div>${title}</div><div class='muted'>${it.ebay_item_id}</div></td>"
    "        <td>${it.currency} ${money(it.total_price)}</td>"
    "        <td>${it.currency} ${money(it.market_value)}</td>"
    "        <td><b>${it.currency} ${money(it.profit)}</b></td>"
    "        <td>${mins}</td>"
    "        <td>${it.query||''}</td>"
    "      </tr>`;"
    "  }"
    "}"
    "load();"
    "</script>"
    "</body></html>"
)


@app.get("/", response_class=HTMLResponse)
def home() -> str:
    return HOME_HTML
