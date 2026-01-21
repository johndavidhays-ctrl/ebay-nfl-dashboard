# app/main.py
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy import select

from .db import SessionLocal, ensure_schema, Item

app = FastAPI()


@app.on_event("startup")
def startup():
    ensure_schema()


@app.get("/")
def home():
    return HTMLResponse(_HTML)


@app.get("/items")
def items():
    now = datetime.now(timezone.utc)
    db = SessionLocal()
    try:
        rows = db.scalars(
            select(Item)
            .where(Item.active.is_(True))
            .where(Item.profit > 0)
            .order_by(Item.end_time)
        ).all()

        out = []
        for it in rows:
            mins = int((it.end_time - now).total_seconds() // 60)
            out.append({
                **it.to_dict(),
                "ends_in": mins
            })

        return {"items": out}
    finally:
        db.close()


_HTML = """
<!doctype html>
<html>
<head>
<title>Auctions Ending Soon</title>
<style>
body{font-family:Arial;margin:20px}
table{border-collapse:collapse;width:100%}
th,td{border:1px solid #ddd;padding:8px}
th{background:#f4f4f4}
img{max-width:90px}
</style>
</head>
<body>
<h2>Auctions Ending Soon</h2>
<table>
<tr>
<th>Card</th>
<th>Total</th>
<th>Market</th>
<th>Profit</th>
<th>Ends In</th>
</tr>
<tbody id="b"></tbody>
</table>
<script>
fetch('/items').then(r=>r.json()).then(d=>{
  const b=document.getElementById('b')
  d.items.forEach(i=>{
    b.innerHTML+=`
    <tr>
      <td><img src="${i.image_url}"><br><a href="${i.url}" target="_blank">${i.title}</a></td>
      <td>$${i.total_price.toFixed(2)}</td>
      <td>$${i.market_value.toFixed(2)}</td>
      <td><b>$${i.profit.toFixed(2)}</b></td>
      <td>${i.ends_in} min</td>
    </tr>`
  })
})
</script>
</body>
</html>
"""
