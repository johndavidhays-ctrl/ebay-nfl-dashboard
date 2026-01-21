from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.db import fetch_active_deals, init_db


app = FastAPI()
engine = init_db()


def _utcnow():
    return datetime.now(timezone.utc)


def _mins_away(dt):
    if not dt:
        return ""
    delta = dt - _utcnow()
    mins = int(delta.total_seconds() // 60)
    return str(mins)


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
def home():
    with Session(engine) as session:
        deals = fetch_active_deals(session, limit=250)

    rows = []
    for d in deals:
        mins = _mins_away(d.end_time)
        rows.append(
            f"""
            <tr>
              <td style="width:220px;">
                <div style="display:flex; gap:10px; align-items:flex-start;">
                  <img src="{d.image_url}" style="width:120px; height:auto; border:1px solid #ddd;" />
                  <div>
                    <a href="{d.url}" target="_blank" rel="noreferrer">{d.title}</a>
                    <div style="color:#666; font-size:12px;">{d.item_id}</div>
                  </div>
                </div>
              </td>
              <td style="white-space:nowrap;">USD {d.total_cost:.2f}</td>
              <td style="white-space:nowrap;">USD {d.market:.2f}</td>
              <td style="white-space:nowrap; font-weight:700;">USD {d.profit:.2f}</td>
              <td style="white-space:nowrap;">{mins}</td>
              <td style="white-space:nowrap; color:#666;">{d.query}</td>
            </tr>
            """
        )

    html = f"""
    <html>
      <head>
        <title>Auctions ending soon</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
      </head>
      <body style="font-family:Arial, sans-serif; margin:20px;">
        <h2>Auctions ending soon</h2>
        <div style="color:#555; margin-bottom:12px;">
          Sorted by nearest end time. Ends is minutes away.
        </div>
        <table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse; width:100%;">
          <thead>
            <tr>
              <th align="left">Card</th>
              <th align="left">Total</th>
              <th align="left">Market</th>
              <th align="left">Profit</th>
              <th align="left">Ends</th>
              <th align="left">Query</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows) if rows else '<tr><td colspan="6">No deals found yet</td></tr>'}
          </tbody>
        </table>
      </body>
    </html>
    """
    return HTMLResponse(html)
