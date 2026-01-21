# app/scanner.py
import os
import re
import json
import base64
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from .db import SessionLocal, ensure_schema, Item


SCANNER_VERSION = "AUCTIONS_SINGLES_FAST_PROFIT_V3"

EBAY_BROWSE_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
EBAY_OAUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_SCOPE = "https://api.ebay.com/oauth/api_scope"


def log(msg: str) -> None:
    print(f"SCANNER: {msg}", flush=True)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_access_token() -> str:
    token = os.getenv("EBAY_OAUTH_TOKEN")
    if token:
        return token

    cid = os.getenv("EBAY_CLIENT_ID")
    sec = os.getenv("EBAY_CLIENT_SECRET")
    if not cid or not sec:
        raise RuntimeError("Missing eBay credentials")

    basic = base64.b64encode(f"{cid}:{sec}".encode()).decode()
    r = requests.post(
        EBAY_OAUTH_URL,
        headers={"Authorization": f"Basic {basic}"},
        data={"grant_type": "client_credentials", "scope": EBAY_SCOPE},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def parse_money(m: Optional[Dict[str, Any]]) -> Tuple[float, str]:
    if not m:
        return 0.0, "USD"
    return float(m.get("value", 0)), m.get("currency", "USD")


def parse_end_time(item: Dict[str, Any]) -> Optional[datetime]:
    v = item.get("itemEndDate")
    if not v:
        return None
    return datetime.fromisoformat(v.replace("Z", "+00:00"))


_lot_re = re.compile(r"\b(lot|bundle|bulk|assortment|\d+\s*cards)\b", re.I)


def is_lot(title: str) -> bool:
    return bool(_lot_re.search(title.lower()))


def estimate_market_value(token: str, title: str, currency: str) -> float:
    clean = re.sub(r"[^a-z0-9 ]", " ", title.lower())
    clean = " ".join(clean.split()[:7])

    r = requests.get(
        EBAY_BROWSE_URL,
        headers={"Authorization": f"Bearer {token}"},
        params={
            "q": clean,
            "filter": "buyingOptions:{FIXED_PRICE}",
            "limit": 40,
            "sort": "price",
        },
        timeout=30,
    )

    if r.status_code != 200:
        return 0.0

    prices = []
    for it in r.json().get("itemSummaries", []):
        p, cur = parse_money(it.get("price"))
        if cur == currency and p > 0:
            prices.append(p)

    if not prices:
        return 0.0

    prices.sort()
    return prices[len(prices) // 2]


def compute_profit(market: float, total: float) -> float:
    fee = market * 0.13
    return round(market - total - fee, 2)


def run() -> None:
    log(SCANNER_VERSION)
    ensure_schema()

    token = get_access_token()
    db = SessionLocal()

    try:
        db.query(Item).update({Item.active: False})
        db.commit()

        queries = [
            "rookie auto",
            "on card auto",
            "numbered card",
            "short print card",
            "ssp football",
        ]

        for q in queries:
            payload = requests.get(
                EBAY_BROWSE_URL,
                headers={"Authorization": f"Bearer {token}"},
                params={
                    "q": q,
                    "filter": "buyingOptions:{AUCTION}",
                    "limit": 200,
                    "sort": "endingSoonest",
                },
                timeout=30,
            ).json()

            for s in payload.get("itemSummaries", []):
                title = s.get("title", "")
                if is_lot(title):
                    continue

                price, cur = parse_money(s.get("price"))
                ship = 0.0
                if price <= 0 or price > 1000:
                    continue

                total = price + ship
                market = estimate_market_value(token, title, cur)

                if market <= 0:
                    continue

                profit = compute_profit(market, total)
                if profit <= 0:
                    continue

                end_time = parse_end_time(s)
                if not end_time:
                    continue

                it = Item(
                    ebay_item_id=s["itemId"],
                    title=title,
                    url=s.get("itemWebUrl", ""),
                    image_url=(s.get("image") or {}).get("imageUrl", ""),
                    query=q,
                    currency=cur,
                    price=price,
                    shipping=ship,
                    total_price=total,
                    end_time=end_time,
                    market_value=market,
                    profit=profit,
                    active=True,
                    raw_json=json.dumps(s),
                )

                db.merge(it)

            db.commit()

        db.query(Item).filter(Item.active.is_(False)).delete()
        db.commit()

    finally:
        db.close()


if __name__ == "__main__":
    run()
