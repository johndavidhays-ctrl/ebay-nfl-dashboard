from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from app.db import init_db, fetch_deals

app = FastAPI()


def money(x):
    if x is None:
        return ""
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return ""


def pct(x):
    if x is None:
        return ""
    try:
        return f"{float(x) * 100:.1f}%"
    except Exception:
        return ""


def num(x):
    if x is None:
        return ""
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return ""


@app.on_event("startup")
def startup():
    init_db()


@app.get("/", response_class=HTMLResponse)
def home():
    deals = fetch_deals(limit=250)

    rows = []
    for d in deals:
        title = d.get("title") or ""
        listing_type = d.get("listing_type") or ""
        buy_price = d.get("buy_price")
        buy_shipping = d.get("buy_shipping")
        est_profit = d.get("est_profit")
        roi = d.get("roi")
        score = d.get("score")

        item_url = d.get("item_url") or ""
        sold_url = d.get("sold_url") or ""

        rows.append(
            f"""
            <tr>
              <td>{title}</td>
              <td>{listing_type}</td>
              <td style="text-align:right">{money(buy_price)}</td>
              <td style="text-align:right">{money(buy_shipping)}</td>
              <td style="text-align:right">{money(est_profit)}</td>
              <td style="text-align:right">{pct(roi)}</td>
              <td style="text-align:right">{num(score)}</td>
              <td>
                <a href="{item_url}" target="_blank" rel="noreferrer">Listing</a>
                |
                <a href="{sold_url}" target="_blank" rel="noreferrer">Sold comps</a>
              </td>
            </tr>
            """
        )

    html = f"""
    <html>
      <head>
        <title>Best Opportunities</title>
        <style>
          body {{ font-family: Arial, sans-serif; padding: 16px; }}
          h1 {{ margin-bottom: 6px; }}
          .sub {{ color: #666; margin-bottom: 14px; }}
          table {{ border-collapse: collapse; width: 100%; }}
          th, td {{ border: 1px solid #ddd; padding: 8px; vertical-align: top; }}
          th {{ background: #f5f5f5; text-align: left; }}
          tr:nth-child(even) {{ background: #fafafa; }}
          a {{ text-decoration: none; }}
        </style>
      </head>
      <body>
        <h1>Best Opportunities</h1>
        <div class="sub">Sorted by score, then estimated profit</div>
        <table>
          <thead>
            <tr>
              <th>Title</th>
              <th>Listing</th>
              <th style="text-align:right">Buy Price</th>
              <th style="text-align:right">Ship</th>
              <th style="text-align:right">Est Profit</th>
              <th style="text-align:right">ROI</th>
              <th style="text-align:right">Score</th>
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
    return HTMLResponse(html)
