import os
import re
import time
from datetime import datetime, timezone
from typing import Any

import requests

from app.db import init_db, mark_all_inactive, prune_inactive, upsert_deal

EBAY_CLIENT_ID = os.getenv("EBAY_CLIENT_ID", "").strip()
EBAY_CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET", "").strip()

if not EBAY_CLIENT_ID or not EBAY_CLIENT_SECRET:
    raise RuntimeError("EBAY_CLIENT_ID or EBAY_CLIENT_SECRET missing")

OAUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
BROWSE_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

MIN_PROFIT = 150.0

EBAY_FEE_RATE = 0.1325
FIXED_FEE = 0.30

NEGATIVE_WORDS = [
    "lot",
    "lots",
    "binder",
    "collection",
    "bulk",
    "mixed",
    "assorted",
    "cards",
    "you get",
    "you will receive",
    "random",
    "team lot",
]


def log(msg: str) -> None:
    print(f"SCANNER: {msg}", flush=True)


def get_token() -> str:
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope",
    }
    r = requests.post(
        OAUTH_URL,
        data=data,
        auth=(EBAY_CLIENT_ID, EBAY_CLIENT_SECRET),
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def parse_money(x: Any) -> float:
    try:
        if not x:
            return 0.0
        v = x.get("value")
        return float(v) if v is not None else 0.0
    except Exception:
        return 0.0


def is_lot(title: str) -> bool:
    t = title.lower()
    for w in NEGATIVE_WORDS:
        if w in t:
            return True
    if re.search(r"\b\d+\s*(card|cards)\b", t):
        return True
    return False


def ebay_search(token: str, q: str, buying_option: str, limit: int = 200) -> list[dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "q": q,
        "limit": str(limit),
        "sort": "endingSoonest" if buying_option == "AUCTION" else "bestMatch",
        "filter": f"buyingOptions:{{{buying_option}}}",
    }

    r = requests.get(BROWSE_SEARCH_URL, headers=headers, params=params, timeout=30)

    if r.status_code == 401:
        raise PermissionError("unauthorized")

    if r.status_code == 429:
        log("Rate limited by eBay. Sleeping 20 seconds.")
        time.sleep(20)
        return []

    if r.status_code >= 500:
        log(f"eBay server error {r.status_code}. Sleeping 10 seconds.")
        time.sleep(10)
        return []

    r.raise_for_status()
    j = r.json()
    return j.get("itemSummaries", []) or []


def simplify_title_for_comps(title: str) -> str:
    t = title
    t = re.sub(r"\([^)]*\)", " ", t)
    t = re.sub(r"\[[^\]]*\]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    keep = []
    for word in t.split():
        if len(keep) >= 10:
            break
        keep.append(word)
    return " ".join(keep)


def robust_market_estimate(token: str, title: str) -> tuple[float, int]:
    comp_q = simplify_title_for_comps(title)

    comps = ebay_search(token, comp_q, "FIXED_PRICE", limit=60)

    prices: list[float] = []
    for c in comps:
        p = parse_money(c.get("price"))
        if p <= 0:
            continue
        prices.append(p)

    if len(prices) < 6:
        return 0.0, len(prices)

    prices.sort()
    cut = max(1, int(len(prices) * 0.2))
    trimmed = prices[cut : len(prices) - cut] if len(prices) - 2 * cut >= 3 else prices

    mid = trimmed[len(trimmed) // 2]
    return float(mid), len(prices)


def compute_profit(market: float, total_cost: float) -> float:
    if market <= 0:
        return -total_cost
    net = (market * (1.0 - EBAY_FEE_RATE)) - FIXED_FEE
    return net - total_cost


def ends_at_from_item(item: dict[str, Any]) -> datetime | None:
    dt = item.get("itemEndDate")
    if not dt:
        return None
    try:
        x = dt.replace("Z", "+00:00")
        out = datetime.fromisoformat(x)
        if out.tzinfo is None:
            out = out.replace(tzinfo=timezone.utc)
        return out.astimezone(timezone.utc)
    except Exception:
        return None


def run() -> None:
    init_db()

    queries = [
        "psa 10 football ssp",
        "psa 10 football 1/1",
        "psa 10 football /10",
        "psa 10 football /25",
        "psa 10 football /50",
        "psa 10 prizm football /10",
        "psa 10 optic football /10",
        "psa 10 contenders auto /99",
        "psa 10 flawless football /99",
    ]

    token = get_token()

    mark_all_inactive()

    seen = 0
    kept = 0

    for q in queries:
        log(f"query: {q}")

        try:
            auctions = ebay_search(token, q, "AUCTION", limit=200)
        except PermissionError:
            log("401 unauthorized. Refreshing token and retrying query once.")
            token = get_token()
            auctions = ebay_search(token, q, "AUCTION", limit=200)

        log(f"items returned: {len(auctions)}")

        comp_calls = 0
        MAX_COMP_CALLS_PER_QUERY = 35

        for item in auctions:
            seen += 1

            title = (item.get("title") or "").strip()
            if not title:
                continue

            if is_lot(title):
                continue

            item_id = item.get("itemId")
            if not item_id:
                continue

            price = parse_money(item.get("price"))
            ship = parse_money(item.get("shippingOptions", [{}])[0].get("shippingCost"))
            total_cost = float(price + ship)

            if total_cost <= 0:
                continue

            ends_at = ends_at_from_item(item)

            market = 0.0
            comp_count = 0

            if comp_calls < MAX_COMP_CALLS_PER_QUERY:
                try:
                    market, comp_count = robust_market_estimate(token, title)
                    comp_calls += 1
                    time.sleep(2)
                except PermissionError:
                    log("401 during comps. Refreshing token once.")
                    token = get_token()
                except Exception:
                    pass

            profit = compute_profit(market, total_cost)

            if profit < MIN_PROFIT:
                continue

            deal = {
                "item_id": item_id,
                "title": title,
                "url": item.get("itemWebUrl"),
                "image_url": (item.get("image") or {}).get("imageUrl"),
                "query": q,
                "total_cost": round(total_cost, 2),
                "market": round(float(market), 2),
                "profit": round(float(profit), 2),
                "ends_at": ends_at,
            }

            upsert_deal(deal)
            kept += 1

        log(f"kept so far: {kept}")

    pruned = prune_inactive(older_than_days=14)

    log(f"seen: {seen}")
    log(f"kept: {kept}")
    log(f"pruned_inactive: {pruned}")


if __name__ == "__main__":
    run()
