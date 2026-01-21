import os
import re
import json
import base64
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import requests

from .db import SessionLocal, ensure_schema, Item


EBAY_BROWSE_SEARCH = "https://api.ebay.com/buy/browse/v1/item_summary/search"
EBAY_OAUTH_TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_SCOPE = "https://api.ebay.com/oauth/api_scope"


MIN_PROFIT = 150.0
FEE_RATE = 0.145

RAW_GRADE_COST = 25.0
RAW_RISK_BUFFER = 40.0

MAX_HOURS_AWAY = 12

LOT_BLOCKLIST = [
    "lot",
    "lots",
    "bundle",
    "bundles",
    "collection",
    "break",
    "breaks",
    "random team",
    "pick your team",
    "you pick",
    "team set",
    "complete set",
    "set break",
]


GRADED_REGEX = re.compile(r"\b(psa\s*10|sgc\s*10|bgs\s*9\.5)\b", re.IGNORECASE)


RAW_UPSIDE_REQUIRED = [
    "ssp",
    "case hit",
    "micro mosaic",
    "color blast",
    "downtown",
    "manga",
    "kaboom",
    "genesis",
]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_money(price_obj: Optional[Dict[str, Any]]) -> float:
    if not price_obj:
        return 0.0
    try:
        return float(price_obj.get("value", 0.0))
    except Exception:
        return 0.0


def get_access_token() -> str:
    existing = os.getenv("EBAY_OAUTH_TOKEN", "").strip()
    if existing:
        return existing

    cid = os.getenv("EBAY_CLIENT_ID", "").strip()
    sec = os.getenv("EBAY_CLIENT_SECRET", "").strip()
    if not cid or not sec:
        raise RuntimeError("Missing EBAY_CLIENT_ID or EBAY_CLIENT_SECRET and no EBAY_OAUTH_TOKEN provided")

    basic = base64.b64encode(f"{cid}:{sec}".encode()).decode()
    r = requests.post(
        EBAY_OAUTH_TOKEN_URL,
        headers={"Authorization": f"Basic {basic}"},
        data={"grant_type": "client_credentials", "scope": EBAY_SCOPE},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    tok = data.get("access_token")
    if not tok:
        raise RuntimeError(f"Could not get access token: {data}")
    return tok


def is_lot(title: str) -> bool:
    t = title.lower()
    return any(w in t for w in LOT_BLOCKLIST)


def lane_for_title(title: str) -> str:
    if GRADED_REGEX.search(title or ""):
        return "graded"
    return "raw"


def looks_like_raw_upside(title: str) -> bool:
    t = (title or "").lower()
    return any(k in t for k in RAW_UPSIDE_REQUIRED)


def iso_to_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def hours_away(end_time: datetime) -> float:
    return (end_time - now_utc()).total_seconds() / 3600.0


def profit_after_fees(market: float, buy: float) -> float:
    # profit = market - fees - buy
    return market - (market * FEE_RATE) - buy


def estimate_market_from_fixed_price(token: str, title: str) -> float:
    # Conservative: use a lower quartile of fixed price listings as a rough floor
    # If we cannot get enough samples, return 0 so it gets skipped
    q = " ".join((title or "").split()[:7]).strip()
    if not q:
        return 0.0

    r = requests.get(
        EBAY_BROWSE_SEARCH,
        headers={"Authorization": f"Bearer {token}"},
        params={
            "q": q,
            "filter": "buyingOptions:{FIXED_PRICE}",
            "limit": 60,
        },
        timeout=30,
    )
    if r.status_code != 200:
        return 0.0

    vals: List[float] = []
    for it in r.json().get("itemSummaries", []):
        p = parse_money(it.get("price"))
        if p > 0:
            vals.append(p)

    if len(vals) < 12:
        return 0.0

    vals.sort()
    return float(vals[len(vals) // 4])  # 25th percentile


def search_auctions(token: str, query: str) -> List[Dict[str, Any]]:
    r = requests.get(
        EBAY_BROWSE_SEARCH,
        headers={"Authorization": f"Bearer {token}"},
        params={
            "q": query,
            "filter": "buyingOptions:{AUCTION}",
            "sort": "endingSoonest",
            "limit": 200,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("itemSummaries", [])


def run() -> None:
    ensure_schema()
    token = get_access_token()

    db = SessionLocal()

    # Mark all inactive, then reactivate only those we keep
    db.query(Item).update({Item.active: False})
    db.commit()

    queries = [
        # Graded locks
        "psa 10 football ssp",
        "psa 10 football case hit",
        "sgc 10 football ssp",
        "bgs 9.5 football ssp",
        "psa 10 micro mosaic",
        "psa 10 color blast",

        # Raw upside
        "raw micro mosaic",
        "raw color blast",
        "raw football case hit",
        "raw football ssp",
        "raw kaboom",
        "raw downtown",
    ]

    kept = 0

    for q in queries:
        items = search_auctions(token, q)

        for s in items:
            title = s.get("title", "") or ""
            if not title:
                continue

            if is_lot(title):
                continue

            end_str = s.get("itemEndDate")
            if not end_str:
                continue
            end_time = iso_to_dt(end_str)

            if hours_away(end_time) > MAX_HOURS_AWAY:
                continue

            buy = parse_money(s.get("price"))
            if buy <= 0:
                continue

            lane = lane_for_title(title)

            if lane == "raw" and not looks_like_raw_upside(title):
                continue

            market = estimate_market_from_fixed_price(token, title)
            if market <= 0:
                continue

            profit = profit_after_fees(market, buy)

            if lane == "raw":
                profit = profit - RAW_GRADE_COST - RAW_RISK_BUFFER

            if profit < MIN_PROFIT:
                continue

            ebay_id = s.get("itemId")
            if not ebay_id:
                continue

            image_url = ""
            img = s.get("image") or {}
            if isinstance(img, dict):
                image_url = img.get("imageUrl", "") or ""

            url = s.get("itemWebUrl", "") or ""

            row = Item(
                ebay_item_id=str(ebay_id),
                title=title,
                url=url,
                image_url=image_url,
                lane=lane,
                total_price=float(buy),
                market_value=float(round(market, 2)),
                profit=float(round(profit, 2)),
                end_time=end_time,
                active=True,
                raw_json=json.dumps(s),
            )
            db.merge(row)
            kept += 1

        db.commit()

    # Remove anything not seen in the last run
    db.query(Item).filter(Item.active.is_(False)).delete()
    db.commit()
    db.close()

    print(f"SCANNER: kept: {kept}")


if __name__ == "__main__":
    run()
