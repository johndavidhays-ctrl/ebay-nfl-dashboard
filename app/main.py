from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from app.db import fetch_deals

app = FastAPI()

@app.get("/", response_class=HTMLResponse)
def home():
    rows = fetch_deals()

    html = """
    <html>
      <head>
        <meta charset="utf-8">
        <title>Card Deals</title>
        <style>
          body { font-family: Arial, sans-serif; margin: 24px; }
          table { border-collapse: collapse; width: 100%; }
          th, td { border: 1px solid #ddd; padding: 8px; vertical-align: top; }
          th { background: #f5f5f5; text-align: left; }
          tr:hover { background: #fafafa; }
          .num { text-align: right; white-space: nowrap; }
          .small { color: #666; font-size: 12px; }
        </style>
      </head>
      <body>
        <h2>Best Opportunities</h2>
        <div class="small">Sorted by score, then estimated profit</div>

        <table>
          <thead>
            <tr>
              <th>Title</th>
              <th>Listing</th>
              <th class="num">Buy Price</th>
              <th class="num">Ship</th>
              <th class="num">Est Profit</th>
              <th class="num">ROI</th>
              <th class="num">Score</th>
              <th>Links</th>
            </tr>
          </thead>
          <tbody>
    """

    def fnum(x, d=2):
        if x is None:
            return ""
        try:
            return f"{float(x):.{d}f}"
        except Exception:
            return ""

    for r in rows:
        title = r.get("title", "")
        item_url = r.get("item_url", "")
        sold_url = r.get("sold_url", "")
        listing_type = r.get("listing_type", "")

        buy_price = r.get("buy_price", 0)
        buy_shipping = r.get("buy_shipping", 0)

        est_profit = r.get("est_profit", None)
        roi = r.get("roi", None)
        score = r.get("score", None)

        html += f"""
          <tr>
            <td>{title}</td>
            <td>{listing_type or ""}</td>
            <td class="num">${fnum(buy_price)}</td>
            <td class="num">${fnum(buy_shipping)}</td>
            <td class="num">${fnum(est_profit)}</td>
            <td class="num">{fnum(roi)}%</td>
            <td class="num">{fnum(score)}</td>
            <td>
              <a href="{item_url}" target="_blank">Listing</a>
              &nbsp;|&nbsp;
              <a href="{sold_url}" target="_blank">Sold comps</a>
            </td>
          </tr>
        """

    html += """
          </tbody>
        </table>
      </body>
    </html>
    """

    return html
