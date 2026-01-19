from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from app.db import init_db, fetch_deals

app = FastAPI()


def money(v):
    try:
        return f"${float(v):,.2f}"
    except Exception:
        return "$0.00"


def pct(v):
    try:
        return f"{float(v):.0f}%"
    except Exception:
        return "0%"


@app.get("/", response_class=HTMLResponse)
def home():
    init_db()
    deals = fetch_deals()

    rows = []
    for d in deals:
        rows.append(f"""
            <tr>
                <td>{d["title"]}</td>
                <td>{d["listing_type"]}</td>
                <td style="text-align:right">{money(d["buy_price"])}</td>
                <td style="text-align:right">{money(d["buy_shipping"])}</td>
                <td style="text-align:right">{money(d["est_profit"])}</td>
                <td style="text-align:right">{pct(d["roi"])}</td>
                <td style="text-align:right">{float(d["score"]):.2f}</td>
                <td>
                    <a href="{d["item_url"]}" target="_blank">Listing</a> |
                    <a href="{d["sold_url"]}" target="_blank">Sold comps</a>
                </td>
            </tr>
        """)

    return HTMLResponse(f"""
    <html>
      <head>
        <title>Best Opportunities</title>
        <style>
          body {{ font-family: Arial; padding: 18px; }}
          table {{ width: 100%; border-collapse: collapse; }}
          th, td {{ border: 1px solid #ddd; padding: 8px; }}
          th {{ background: #f4f4f4; }}
          tr:nth-child(even) {{ background: #fafafa; }}
        </style>
      </head>
      <body>
        <h1>Best Opportunities</h1>
        <div>Minimum $20 estimated profit</div>
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
    """)
