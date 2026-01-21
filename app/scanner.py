import os
import re
import time
from datetime import datetime, timezone
from statistics import median
from typing import Any

import requests

from app.db import init_db, mark_all_inactive, prune_inactive, upsert_deal


SCANNER_VERSION = "AUCTIONS_SINGLES_MINPROFIT150_V1"

EBAY_OAUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
BROWSE_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

DEFAULT_QUERIES = [
    "psa 10 football ssp",
    "psa 10 football 1/1",
    "psa 10 football /10",
    "psa 10 football /25",
    "psa 10 football /50",
    "psa 10 prizm black",
    "psa 10 kaboom",
    "psa 10 downtown",
    "psa 10 color blast",
    "psa 10 gold vinyl",
    "psa 10 on card auto /25",
]

LOT_BAD_WORDS = [
    "lot",
    "binder",
    "collection",
    "bulk",
    "packs",
    "pack",
    "box",
    "case",
    "break",
    "breaks",
    "team set",
    "player lot",
    "mystery",
]

STOP_WORDS = {
    "hot", "rare", "nice", "great", "awesome", "mint", "gem", "mt", "rc", "rookie",
    "card", "cards", "auto", "autograph", "autograoh", "on", "a", "an", "the", "of",
    "and", "with", "for", "to", "by", "panini", "topps"
}


def log(msg: str) -> None:
    print(f"SCANNER: {msg}", flush=True)


def get_env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


def get_oauth_token() -> str:
    client_id = os.environ.get("EBAY_CLIENT_ID")
    client_secret = os.environ.get("EBAY_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("Missing EBAY_CLIENT_ID or EBAY_CLIENT_SECRET")

    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope",
    }
    r = requests.post(EBAY_OAUTH_URL, auth=auth, data=data, timeout=30)
    r.raise_for_status()
    j = r.json()
    return j["access_token"]


def is_lot(title: str) -> bool:
    t = title.lower()
    if re.search(r"\b\d+\s*cards?\b", t):
        return True
    for w in LOT_BAD_WORDS:
        if w in t:
            return True
    return False


def parse_money(obj: Any) -> float:
    if not obj:
        return 0.0
    try:
        v = obj.get("value")
        if v is None:
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def parse_end_date(item: dict[str, Any]) -> datetime | None:
    s = item.get("itemEndDate")
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


def build_comp_query(title: str) -> str:
    t = re.sub(r"[^A-Za-z0-9\s/#]+", " ", title)
    t = re.sub(r"\s+", " ", t).strip()

    tokens = []
    for raw in t.split(" "):
        low = raw.lower()
        if low in STOP_WORDS:
            continue
        if len(low) <= 1:
            continue
        tokens.append(raw)

    keep = tokens[:10]
    return " ".join(keep) if keep else title


def ebay_search(token: str, q: str, buying_option: str, limit: int = 200) -> list[dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    params = {
        "q": q,
        "limit": str(limit),
        "sort": "endingSoonest" if buying_option == "AUCTION" else "bestMatch",
        "filter": f"buyingOptions:{{{buying_option}}}",
    }
    r = requests.get(BROWSE_SEARCH_URL, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    return j.get("itemSummaries", []) or []


def total_cost_from_item(item: dict[str, Any]) -> float:
    price = parse_money(item.get("price"))
    ship = 0.0
    ship_opts = item.get("shippingOptions") or []
    if ship_opts:
        ship = parse_money((ship_opts[0] or {}).get("shippingCost"))
    return float(price + ship)


def estimate_market_from_fixed_price(token: str, title: str) -> tuple[float, int]:
    comp_q = build_comp_query(title)

    items = ebay_search(token, comp_q, "FIXED_PRICE", limit=50)

    prices: list[float] = []
    for it in items:
        cost = total_cost_from_item(it)
        if cost > 0:
            prices.append(cost)

    if len(prices) < 8:
        return 0.0, len(prices)

    prices.sort()
    trim = int(len(prices) * 0.2)
    trimmed = prices[trim: len(prices) - trim] if len(prices) - trim > trim else prices
    return float(median(trimmed)), len(prices)


def run() -> None:
    log(f"SCANNER VERSION: {SCANNER_VERSION}")

    min_profit = float(get_env_int("MIN_PROFIT", 150))
    max_rows = int(get_env_int("MAX_ROWS", 200))
    sleep_s = float(get_env_int("REQUEST_SLEEP_SECONDS", 1))

    queries_env = os.environ.get("QUERIES_JSON")
    if queries_env:
        try:
            import json
            queries = json.loads(queries_env)
            if not isinstance(queries, list) or not queries:
                queries = DEFAULT_QUERIES
        except Exception:
            queries = DEFAULT_QUERIES
    else:
        queries = DEFAULT_QUERIES

    init_db()
    mark_all_inactive()

    token = get_oauth_token()

    kept = 0
    seen = 0

    for q in queries:
        log(f"query: {q}")

        try:
            auction_items = ebay_search(token, q, "AUCTION", limit=max_rows)
        except Exception as e:
            log(f"search failed for query {q}: {e}")
            continue

        log(f"items returned: {len(auction_items)}")

        for item in auction_items:
            seen += 1

            title = (item.get("title") or "").strip()
            if not title:
                continue

            if is_lot(title):
                continue

            item_id = item.get("itemId")
            if not item_id:
                continue

            url = item.get("itemWebUrl")
            image_url = None
            img = item.get("image") or {}
            if isinstance(img, dict):
                image_url = img.get("imageUrl")

            ends_at_dt = parse_end_date(item)
            if ends_at_dt is None:
                continue

            total_cost = total_cost_from_item(item)
            if total_cost <= 0:
                continue

            market, comp_count = estimate_market_from_fixed_price(token, title)
            if market <= 0:
                continue

            profit = market - total_cost
            if profit < min_profit:
                continue

            deal = {
                "item_id": str(item_id),
                "title": title,
                "url": url,
                "image_url": image_url,
                "query": q,
                "total_cost": float(total_cost),
                "market": float(market),
                "profit": float(profit),
                "ends_at": ends_at_dt,
            }

            upsert_deal(deal)
            kept += 1

            time.sleep(sleep_s)

        time.sleep(sleep_s)

    prune_inactive()
    log(f"seen: {seen}")
    log(f"kept: {kept}")


if __name__ == "__main__":
    run()
