from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from app.db import init_db, fetch_deals

app = FastAPI()


def money(val):
    if val is None:
        return ""
    return f"${float(val):,.2f}"


def percent(val):
    if val is None:
        return ""
    return f"{float(val) * 100:.1f}%"


def number(val):
    if val is None:
        return ""
    return f"{float(val):,.2f}"


@app.on_event("startup")
def startup():
    init_db()


@app.get("/", response_class=HTMLResponse)
def home():
    deals = fetch_deals(limit=250)

    rows_html = ""

    for d in deals:
        rows_html += f"""
        <tr>
            <td>{d["title"]}</td>
            <td>{d.get("listing_type","")}</td>
            <td style="text-align:right">{money(d["buy_price"])}</td>
            <td style="text-align:right">{money(d["buy_shipping"])}</td>
            <td style="text-align:right">{money(d.get("est_profit"))}</td>
            <td style="text-align:right">{percent(d.get("roi"))}</td>
            <td style="text-align:right">{number(d.get("score"))}</td>
            <td>
                <a href="{d["item_url"]}" target="_blank">Listing</a>
                |
                <a href="{d["sold_url"]}" target="_blank">Sold comps</a>
            </td>
        </tr>
        """

    html = f"""
    <html>
    <head>
        <title>Best Opportunities</title>
        <style>
            body {{ font-family: Arial, sans-serif; padding: 16px; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; }}
            th {{ background-color: #f5f5f5; }}
            tr:nth-child(even) {{ background-color: #fafafa; }}
            td {{ vertical-align: top; }}
        </style>
    </head>
    <body>
        <h1>Best Opportunities</h1>
        <p>Sorted by score, then estimated profit</p>
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
                {rows_html}
            </tbody>
        </table>
    </body>
    </html>
    """

    return HTMLResponse(html)
