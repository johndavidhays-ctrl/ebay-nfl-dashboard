from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from app.db import init_db, fetch_deals

app = FastAPI()

@app.on_event("startup")
def startup():
    init_db()

@app.get("/", response_class=HTMLResponse)
def home():
    deals = fetch_deals(200)

    rows = []
    for d in deals:
        rows.append(f"""
        <tr>
            <td>{d["title"]}</td>
            <td>{d.get("listing_type","")}</td>
            <td>${float(d["buy_price"] or 0):.2f}</td>
            <td>${float(d["buy_shipping"] or 0):.2f}</td>
            <td>${float(d["est_profit"] or 0):.2f}</td>
            <td>{float(d["roi"] or 0):.1f}%</td>
            <td>{float(d["score"] or 0):.1f}</td>
            <td>
                <a href="{d["item_url"]}" target="_blank">Listing</a> |
                <a href="{d["sold_url"]}" target="_blank">Sold comps</a>
            </td>
        </tr>
        """)

    html = f"""
    <html>
    <head>
        <title>Best Opportunities</title>
        <style>
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ccc; padding: 6px; }}
            th {{ background: #eee; }}
        </style>
    </head>
    <body>
        <h1>Best Opportunities</h1>
        <p>Sorted by score, then estimated profit</p>
        <table>
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
            {''.join(rows)}
        </table>
    </body>
    </html>
    """
    return html
