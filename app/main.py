from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from app.db import init_db, fetch_deals


app = FastAPI()


def _money(x):
    if x is None:
        return ""
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return ""


def _pct(x):
    if x is None:
        return ""
    try:
        return f"{float(x) * 100:.0f}%"
    except Exception:
        return ""


def _num(x):
    if x is None:
        return ""
    try:
        return f"{float(x):.1f}"
    except Exception:
        return ""


@app.get("/", response_class=HTMLResponse)
def home():
    init_db()
    rows = fetch_deals(limit=250)

    html = []
    html.append("<html><head><meta charset='utf-8'>")
    html.append("<title>Best Opportunities</title>")
    html.append(
        """
        <style>
          body { font-family: Arial, sans-serif; margin: 24px; }
          h1 { margin-bottom: 6px; }
          .sub { color: #555; margin-bottom: 14px; }
          table { border-collapse: collapse; width: 100%; }
          th, td { border: 1px solid #ddd; padding: 8px; vertical-align: top; }
          th { background: #f5f5f5; text-align: left; }
          td.num, th.num { text-align: right; white-space: nowrap; }
          td.links { white-space: nowrap; }
          .muted { color: #777; }
        </style>
        """
    )
    html.append("</head><body>")
    html.append("<h1>Best Opportunities</h1>")
    html.append("<div class='sub'>Sorted by score, then estimated profit</div>")

    html.append("<table>")
    html.append(
        """
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
    )

    for r in rows:
        title = (r.get("title") or "").replace("<", "&lt;").replace(">", "&gt;")
        listing_type = (r.get("listing_type") or "").replace("<", "&lt;").replace(">", "&gt;")
        item_url = r.get("item_url") or ""
        sold_url = r.get("sold_url") or ""

        html.append("<tr>")
        html.append(f"<td>{title}</td>")
        html.append(f"<td class='muted'>{listing_type}</td>")
        html.append(f"<td class='num'>{_money(r.get('buy_price'))}</td>")
        html.append(f"<td class='num'>{_money(r.get('buy_shipping'))}</td>")
        html.append(f"<td class='num'>{_money(r.get('est_profit'))}</td>")
        html.append(f"<td class='num'>{_pct(r.get('roi'))}</td>")
        html.append(f"<td class='num'>{_num(r.get('score'))}</td>")
        html.append(
            "<td class='links'>"
            f"<a href='{item_url}' target='_blank'>Listing</a>"
            " | "
            f"<a href='{sold_url}' target='_blank'>Sold comps</a>"
            "</td>"
        )
        html.append("</tr>")

    html.append("</tbody></table>")

    if not rows:
        html.append("<p class='muted'>No rows found yet. Let the scanner run again.</p>")

    html.append("</body></html>")
    return HTMLResponse("".join(html))
