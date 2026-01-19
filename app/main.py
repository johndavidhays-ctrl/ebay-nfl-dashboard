from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from app.db import init_db, fetch_deals, update_status

app = FastAPI()

@app.on_event("startup")
def startup():
    init_db()

@app.get("/", response_class=HTMLResponse)
def home():
    rows = fetch_deals(limit=200)

    rows_html = ""
    for r in rows:
        rows_html += f"""
        <tr>
          <td>{r['title']}</td>
          <td>{r['buy_price']}</td>
          <td>{r['buy_shipping']}</td>
          <td>{r['seller_feedback_score']}</td>
          <td>{r['status']}</td>
          <td><a href="{r['item_url']}" target="_blank">listing</a></td>
          <td><a href="{r['sold_url']}" target="_blank">sold comps</a></td>
          <td>
            <a href="/set_status?item_id={r['item_id']}&status=bought">bought</a>
            |
            <a href="/set_status?item_id={r['item_id']}&status=ignore">ignore</a>
          </td>
        </tr>
        """

    return f"""
    <html>
    <body>
      <h2>NFL card leads</h2>
      <table border="1" cellpadding="6">
        <tr>
          <th>Title</th>
          <th>Price</th>
          <th>Ship</th>
          <th>Fb %</th>
          <th>Fb score</th>
          <th>Status</th>
          <th>Listing</th>
          <th>Comps</th>
          <th>Action</th>
        </tr>
        {rows_html}
      </table>
    </body>
    </html>
    """

@app.get("/set_status")
def set_status(item_id: str, status: str):
    update_status(item_id, status)
    return {"ok": True}
