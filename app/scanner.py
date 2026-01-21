# app/scanner.py
import os
import re
import json
import base64
import requests
from datetime import datetime, timezone

from sqlalchemy import select
from .db import SessionLocal, ensure_schema, Item

EBAY_SEARCH = "https://api.ebay.com/buy/browse/v1/item_summary/search"
EBAY_TOKEN = "https://api.ebay.com/identity/v1/oauth2/token"
SCOPE = "https://api.ebay.com/oauth/api_scope"

MIN_PROFIT = 150
GRADE_COST = 25
RAW_RISK_BUFFER = 40
FEE_RATE = 0.145


def get_token():
    tok = os.getenv("EBAY_OAUTH_TOKEN")
    if tok:
        return tok
    cid = os.getenv("EBAY_CLIENT_ID")
    sec = os.getenv("EBAY_CLIENT_SECRET")
    basic = base64.b64encode(f"{cid}:{sec}".encode()).decode()
    r = requests.post(
        EBAY_TOKEN,
        headers={"Authorization": f"Basic {basic}"},
        data={"grant_type": "client_credentials", "scope": SCOPE},
        timeout=30,
    )
    return r.json()["access_token"]


def parse_money(obj):
    try:
        return float(obj.get("value", 0))
    except Exception:
        return 0.0


def estimate_market(token, title):
    q = " ".join(title.split()[:6])
    r = requests.get(
        EBAY_SEARCH,
        headers={"Authorization": f"Bearer {token}"},
        params={"q": q, "filter": "buyingOptions:{FIXED_PRICE}", "limit": 40},
        timeout=30,
    )
    vals = []
    for it in r.json().get("itemSummaries", []):
        p = parse_money(it.get("price"))
        if p > 0:
            vals.append(p)
    if len(vals) < 10:
        return 0.0
    vals.sort()
    return vals[len(vals) // 4]


def profit_calc(market, buy):
    return round(market - buy - (market * FEE_RATE), 2)


def run():
    ensure_schema()
    token = get_token()
    db = SessionLocal()
    db.query(Item).update({Item.active: False})
    db.commit()

    queries = [
        "psa 10 ssp football",
        "sgc 10 case hit",
        "bgs 9.5 micro mosaic",
        "raw micro mosaic",
        "raw color blast",
        "raw case hit football",
    ]

    for q in queries:
        r = requests.get(
            EBAY_SEARCH,
            headers={"Authorization": f"Bearer {token}"},
            params={"q": q, "filter": "buyingOptions:{AUCTION}", "sort": "endingSoonest", "limit": 200},
            timeout=30,
        )
        for s in r.json().get("itemSummaries", []):
            title = s.get("title", "")
            buy = parse_money(s.get("price"))
            if buy <= 0:
                continue

            graded = bool(re.search(r"psa 10|sgc 10|bgs 9\.5", title.lower()))
            raw = not graded

            market = estimate_market(token, title)
            if market <= 0:
                continue

            profit = profit_calc(market, buy)
            lane = "graded"

            if raw:
                profit -= (GRADE_COST + RAW_RISK_BUFFER)
                lane = "raw"

            if profit < MIN_PROFIT:
                continue

            it = Item(
                ebay_item_id=s["itemId"],
                title=title,
                url=s.get("itemWebUrl", ""),
                image_url=(s.get("image") or {}).get("imageUrl", ""),
                lane=lane,
                total_price=buy,
                market_value=market,
                profit=profit,
                end_time=datetime.fromisoformat(s["itemEndDate"].replace("Z", "+00:00")),
                active=True,
                raw_json=json.dumps(s),
            )
            db.merge(it)

        db.commit()

    db.query(Item).filter(Item.active.is_(False)).delete()
    db.commit()
    db.close()


if __name__ == "__main__":
    run()
