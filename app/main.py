from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from app.db import init_db, fetch_deals

app = FastAPI()


def money(x):
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return "$0.00"


def pct(x):
    try:
        return f"{float(x):.0f}%"
    except Exception:
        return "0%"


@app.get("/", response_class=HTMLResponse)
def home():
    init_db()
    deals = fetch_deals(limit=250)

    rows = []
    for d in deals:
        title = d.get("title", "")
        listing = d.get("listing_type", "") or ""
        buy_price = money(d.get("buy_price", 0))
        ship = money(d.get("buy_shipping", 0))
        est_profit = money(d.get("est_profit", 0))
        roi = pct(d.get("roi", 0))
        score = f"{float(d.get('score', 0)):.2f}"

        item_url = d.get("item_url", "")
        sold_url = d.get("sold_url", "")

        links = ""
        if item_url:
            links += f'<a href="{item_url}" target="_blank">Listing</a>'
        if sold_url:
            if links:
                links += " | "
            links += f'<a href="{sold_url}" target="_blank">Sold comps</a>'

        rows.append(
            f"""
            <tr>
                <td>{title}</td>
                <td>{listing}</td>
                <td style="text-align:right">{buy_price}</td>
                <td style="text-align:right">{ship}</td>
                <td style="text-align:right">{est_profit}</td>
                <td style="text-align:right">{roi}</td>
                <td style="text-align:right">{score}</td>
                <td>{links}</td>
            </tr>
            """
        )

    html = f"""
    <html>
      <head>
        <title>Best Opportunities</title>
        <style>
          body {{ font-family: Arial, sans-serif; padding: 18px; }}
          table {{ border-collapse: collapse; width: 100%; }}
          th, td {{ border: 1px solid #ddd; padding: 8px; }}
          th {{ background: #f4f4f4; text-align: left; }}
          tr:nth-child(even) {{ background: #fafafa; }}
        </style>
      </head>
      <body>
        <h1>Best Opportunities</h1>
        <div>Sorted by score, then estimated profit</div>
        <br/>
        <table>
          <thead>
            <tr>
              <th>Title</th>
              <th>Listing</th>
              <th>Buy Price</th>
              <th>Ship</th>
              <th>Est Profit</th>
              <th>ROI</th>
              <th>Score</th>
              <th>Links</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows)}
          </tbody>
        </table>
      </body>
    </html>
    """
    return HTMLResponse(content=html)
