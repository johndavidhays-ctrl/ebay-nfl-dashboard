from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from app.db import fetch_active_deals, init_db

app = FastAPI()


@app.on_event("startup")
def _startup() -> None:
    init_db()


def minutes_away(ends_at_iso: str | None) -> int | None:
    if not ends_at_iso:
        return None
    try:
        dt = datetime.fromisoformat(ends_at_iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return max(0, int((dt - now).total_seconds() // 60))
    except Exception:
        return None


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "nfl card dashboard",
        "utc": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/deals")
def deals(limit: int = 200) -> JSONResponse:
    rows = fetch_active_deals(limit=limit)
    for r in rows:
        r["minutes_away"] = minutes_away(r.get("ends_at"))
    rows.sort(key=lambda x: (x["minutes_away"] is None, x["minutes_away"] or 10**9))
    return JSONResponse(rows)


@app.get("/", response_class=HTMLResponse)
def home() -> HTMLResponse:
    rows = fetch_active_deals(limit=200)
    for r in rows:
        r["minutes_away"] = minutes_away(r.get("ends_at"))
    rows.sort(key=lambda x: (x["minutes_away"] is None, x["minutes_away"] or 10**9))

    def money(v: float) -> str:
        return f"${v:,.2f}"

    html_rows = []
    for r in rows:
        if r.get("profit", 0) <= 0:
            continue

        img = ""
        if r.get("image_url"):
            img = f"<img src='{r['image_url']}' style='max-height:120px;max-width:90px;border-radius:6px' />"

        link = r.get("title", "view")
        url = r.get("url") or "#"
        title_html = f"<a href='{url}' target='_blank' rel='noreferrer'>{link}</a>"

        html_rows.append(
            f"""
            <tr>
                <td style="width:110px">{img}</td>
                <td>{title_html}<div style="opacity:.7;font-size:12px">{r.get("item_id","")}</div></td>
                <td style="text-align:right;white-space:nowrap">{money(float(r.get("total_cost",0)))}</td>
                <td style="text-align:right;white-space:nowrap">{money(float(r.get("market",0)))}</td>
                <td style="text-align:right;white-space:nowrap;font-weight:700">{money(float(r.get("profit",0)))}</td>
                <td style="text-align:right;white-space:nowrap">{r.get("minutes_away") if r.get("minutes_away") is not None else ""}</td>
                <td style="white-space:nowrap">{r.get("query") or ""}</td>
            </tr>
            """
        )

    page = f"""
    <html>
    <head>
        <meta charset="utf-8" />
        <title>Auctions ending soon</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 24px; }}
            h1 {{ margin: 0 0 8px 0; }}
            .sub {{ margin: 0 0 16px 0; opacity: .75; }}
            table {{ width: 100%; border-collapse: collapse; }}
            th, td {{ border-bottom: 1px solid #ddd; padding: 10px; vertical-align: top; }}
            th {{ text-align: left; position: sticky; top: 0; background: #fff; }}
        </style>
    </head>
    <body>
        <h1>Auctions ending soon</h1>
        <p class="sub">Sorted by minutes away. Only shows positive profit rows.</p>
        <table>
            <thead>
                <tr>
                    <th>Card</th>
                    <th></th>
                    <th>Total</th>
                    <th>Market</th>
                    <th>Profit</th>
                    <th>Minutes</th>
                    <th>Query</th>
                </tr>
            </thead>
            <tbody>
                {''.join(html_rows)}
            </tbody>
        </table>
    </body>
    </html>
    """
    return HTMLResponse(page)
