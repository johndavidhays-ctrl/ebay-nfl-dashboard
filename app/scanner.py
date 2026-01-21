# app/scanner.py

import os
import time
import random
from datetime import datetime, timezone
import requests

from app.db import init_db, upsert_deal, mark_all_inactive, prune_inactive


SCANNER_VERSION = "AUCTIONS_SINGLES_MINPROFIT150_STABLE_V1"

EBAY_OAUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

MIN_PROFIT = 150.0
MAX_AUCTIONS_PER_QUERY = 25
MAX_COMP_CALLS = 5

# Keep this VERY small to avoid bans
QUERIES = [
    "psa 10 football auto /10",
    "psa 10 football auto /25",
    "psa 10 football 1/1",
]


def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"{ts} SCANNER: {msg}", flush=True)


def get_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var {name}")
    return v


def get_token() -> str:
    r = requests.post(
        EBAY_OAUTH_URL,
        auth=(get_env("EBAY_CLIENT_ID"), get_env("EBAY_CLIENT_SECRET")),
        data={
            "grant_type": "client_credentials",
            "scope": "https://api.ebay.com/oauth/api_scope",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def ebay_search(token: str, query: str, buying: str, limit: int):
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "q": query,
        "limit": limit,
        "filter": f"buyingOptions:{{{buying}}}",
        "sort": "endingSoonest",
    }

    r = requests.get(EBAY_SEARCH_URL, headers=headers, params=params, timeout=30)

    if r.status_code == 429:
        log("Rate limited by eBay. Ending scan cleanly.")
        return None

    if r.status_code >= 400:
        log(f"eBay error {r.status_code}. Skipping query.")
        return []

    return r.json().get("itemSummaries", [])


def price(item):
    p = item.get("currentBidPrice") or item.get("price") or {}
    return float(p.get("value", 0.0))


def run():
    log(f"SCANNER VERSION: {SCANNER_VERSION}")

    init_db()
    token = get_token()

    mark_all_inactive()

    seen = kept = 0
    comp_calls = 0

    for q in QUERIES:
        log(f"query: {q}")
        time.sleep(2)

        auctions = ebay_search(token, q, "AUCTION", MAX_AUCTIONS_PER_QUERY)
        if auctions is None:
            break

        log(f"items returned: {len(auctions)}")

        for a in auctions:
            seen += 1
            title = a.get("title", "")
            if not title or "PSA" not in title.upper():
                continue

            total = price(a)
            if total <= 0:
                continue

            if comp_calls >= MAX_COMP_CALLS:
                continue

            time.sleep(3)
            comps = ebay_search(token, title, "FIXED_PRICE", 20)
            comp_calls += 1

            if not comps:
                continue

            prices = sorted(price(c) for c in comps if price(c) > 0)
            if len(prices) < 5:
                continue

            market = prices[len(prices) // 2]
            profit = market - total

            if profit < MIN_PROFIT:
                continue

            kept += 1

            upsert_deal(
                {
                    "item_id": a.get("itemId"),
                    "title": title,
                    "url": a.get("itemWebUrl"),
                    "image_url": (a.get("image") or {}).get("imageUrl"),
                    "query": q,
                    "total_cost": total,
                    "market": market,
                    "profit": profit,
                    "ends_at": a.get("itemEndDate"),
                    "is_active": True,
                }
            )

        log(f"kept so far: {kept}")

    pruned = prune_inactive()
    log(f"seen: {seen}")
    log(f"kept: {kept}")
    log(f"pruned: {pruned}")
    log("scan completed cleanly")


if __name__ == "__main__":
    run()
