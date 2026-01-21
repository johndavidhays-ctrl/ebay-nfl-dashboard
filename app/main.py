import os
from datetime import datetime, timezone
from typing import List, Dict, Any

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy import select, text

from .db import SessionLocal, ensure_schema, Item, DATABASE_URL

app = FastAPI()


@app.on_event("startup")
def startup() -> None:
    ensure_schema()


@app.get("/debug")
def debug() -> Dict[str, Any]:
    ensure_schema()
    db = SessionLocal()
    try:
        host_part = (DATABASE_URL or "").split("@")[-1].split("?")[0]
        total = db.execute(text("select count(*) from items")).scalar() or 0
        active = db.execute(text("select count(*) from items where active = true")).scalar() or 0
        lanes = db.execute(text("select lane, count(*) from items group by lane")).fetchall()
        return {
            "db": host_part,
            "total": int(total),
            "active": int(active),
            "lanes": {str(k): int(v) for k, v in lanes},
        }
    finally:
        db.close()


@app.get("/items")
def items() -> Dict[str, Any]:
    ensure_schema()
    db = SessionLocal()
    try:
        now = datetime.now(timezone.utc)

        rows: List[Item] = db.scalars(
            select(Item)
            .where(Item.active.is_(True))
            .order_by(Item.end_time.asc())
        ).all()

        out = []
        for it in rows:
            mins = None
            if it.end_time:
                mins = int((it.end_time - now).total_seconds() // 60)
                if mins < 0:
                    mins = 0

            d = it.to_dict()
            d["ends_minutes"] = mins
            out.append(d)

        return {"items": out}
    finally:
        db.close()


@app.get("/")
def home() -> HTMLResponse:
    return HTMLResponse(HTML)


HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Card Sniper Board</title>
  <style>
    body { font-family: Arial; margin: 20px; }
    h1 { margin-bottom: 6px; }
    .sub { color: #444; margin-bottom: 20px; }
    h2 { margin-top: 28px; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #ddd; padding: 8px; vertical-align: top; }
    th { background: #f4f4f4; }
    img { max-width: 90px; height: auto; display: block; }
    .laneRaw { background: #fff3e0; }
    .muted { color: #666; font-size: 12px; }
  </style>
</head>
<body>
  <h1>Card Sniper Board</h1>
  <div class="sub">Two lanes. Auction only. Singles only. Profit 150 plus. Sorted by ending soonest.</div>

  <h2>Graded Locks</h2>
  <table id="graded">
    <tr>
      <th>Card</th>
      <th>Buy</th>
      <th>Market</th>
      <th>Profit</th>
      <th>Ends</th>
    </tr>
  </table>

  <h2>Raw Upside</h2>
  <table id="raw">
    <tr>
      <th>Card</th>
      <th>Buy</th>
      <th>Market</th>
      <th>Profit</th>
      <th>Ends</th>
    </tr>
  </table>

<script>
function money(v) {
  if (v === null || v === undefined) return '';
  return '$' + Number(v).toFixed(2);
}

fetch('/items')
  .then(r => r.json())
  .then(data => {
    const graded = document.getElementById('graded');
    const raw = document.getElementById('raw');

    (data.items || []).forEach(i => {
      const cls = (i.lane === 'raw') ? 'laneRaw' : '';
      const ends = (i.ends_minutes === null || i.ends_minutes === undefined) ? '' : (i.ends_minutes + ' min');

      const row = document.createElement('tr');
      row.className = cls;

      row.innerHTML = `
        <td>
          <img src="${i.image_url || ''}" alt="">
          <a href="${i.url || '#'}" target="_blank">${i.title || ''}</a>
          <div class="muted">${i.lane === 'raw' ? 'Raw Upside' : 'Graded Lock'}</div>
        </td>
        <td>${money(i.total_price)}</td>
        <td>${money(i.market_value)}</td>
        <td><b>${money(i.profit)}</b></td>
        <td>${ends}</td>
      `;

      if (i.lane === 'raw') raw.appendChild(row);
      else graded.appendChild(row);
    });
  })
  .catch(err => {
    document.body.insertAdjacentHTML('beforeend', '<p style="color:red">Error loading items. Check /debug and Render logs.</p>');
    console.error(err);
  });
</script>

</body>
</html>
"""
