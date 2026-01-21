# app/main.py
from datetime import datetime, timezone
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy import select

from .db import SessionLocal, ensure_schema, Item

app = FastAPI()


@app.on_event("startup")
def start():
    ensure_schema()


@app.get("/")
def home():
    return HTMLResponse(HTML)


@app.get("/items")
def items():
    db = SessionLocal()
    now = datetime.now(timezone.utc)
    rows = db.scalars(
        select(Item).where(Item.active.is_(True)).order_by(Item.end_time)
    ).all()
    out = []
    for it in rows:
        mins = int((it.end_time - now).total_seconds() // 60)
        out.append({**it.to_dict(), "ends": mins})
    return {"items": out}


HTML = """
<!doctype html>
<html>
<head>
<style>
body{font-family:Arial;margin:20px}
h2{margin-top:30px}
table{border-collapse:collapse;width:100%}
th,td{border:1px solid #ddd;padding:8px}
th{background:#f4f4f4}
.raw{background:#fff3e0}
img{max-width:90px}
</style>
</head>
<body>
<h1>Card Sniper Board</h1>

<h2>Graded Locks</h2>
<table id="graded"></table>

<h2>Raw Upside</h2>
<table id="raw"></table>

<script>
fetch('/items').then(r=>r.json()).then(d=>{
  const g=document.getElementById('graded')
  const r=document.getElementById('raw')
  g.innerHTML='<tr><th>Card</th><th>Buy</th><th>Market</th><th>Profit</th><th>Ends</th></tr>'
  r.innerHTML=g.innerHTML
  d.items.forEach(i=>{
    const row=`
      <tr class="${i.lane}">
        <td><img src="${i.image_url}"><br><a href="${i.url}" target="_blank">${i.title}</a></td>
        <td>$${i.total_price}</td>
        <td>$${i.market_value}</td>
        <td><b>$${i.profit}</b></td>
        <td>${i.ends} min</td>
      </tr>`
    if(i.lane==='graded'){g.innerHTML+=row}else{r.innerHTML+=row}
  })
})
</script>
</body>
</html>
"""
