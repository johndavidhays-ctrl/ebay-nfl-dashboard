import os
import time
import math
import json
import requests
import psycopg2
from urllib.parse import quote_plus

from app.db import get_conn, init_db


SCANNER_VERSION = "SINGLES_ONLY_UPSIDE_100_ACTIVE_PRUNE_V1"

EBAY_CLIENT_ID = os.getenv("EBAY_CLIENT_ID", "")
EBAY_CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET", "")
EBAY_APP_ID = os.getenv("EBAY_APP_ID", "")  # only needed for sold comps via Finding API
EBAY_MARKETPLACE_ID = os.getenv("EBAY_MARKETPLACE_ID", "EBAY_US")

DATABASE_URL = os.getenv("DATABASE_URL", "")

# Focus settings
MAX_BUY_PRICE = 1000.0
MAX_ITEMS_PER_QUERY = 200
MIN_EST_PROFIT = 20.0
PAUSE_BETWEEN_CALLS_SEC = 0.35

# Include auctions and fixed price
BUYING_OPTIONS = ["FIXED_PRICE", "AUCTION"]

# Singles only and broad enough to find hidden value
SEARCH_QUERIES = [
    "sports card",
    "rookie card",
    "autograph card",
    "patch card",
    "numbered card",
    "refractor card",
    "parallel card",
    "short print card",
]

# Basic negative keywords to avoid lots
NEGATIVE_KEYWORDS = [
    "lot",
    "lots",
    "bundle",
    "bulk",
    "pack",
    "packs",
    "box",
    "case",
    "break",
    "breaks",
]


def log(msg: str):
    print(f"SCANNER: {msg}", flush=True)


def get_oauth_token() -> str:
    if not EBAY_CLIENT_ID or not EBAY_CLIENT_SECRET:
        raise RuntimeError("Missing EBAY_CLIENT_ID or EBAY_CLIENT_SECRET")

    url = "https://api.ebay.com/identity/v1/oauth2/token"
    auth = (EBAY_CLIENT_ID, EBAY_CLIENT_SECRET)
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope",
    }
    r = requests.post(url, auth=auth, data=data, timeout=30)
    log(f"EBAY OAUTH STATUS: {r.status_code}")
    r.raise_for_status()
    token = r.json().get("access_token")
    if not token:
        raise RuntimeError("No access_token in eBay OAuth response")
    return token


def build_filter_string():
    parts = []

    # Buying options
    if BUYING_OPTIONS:
        parts.append("buyingOptions:{" + ",".join(BUYING_OPTIONS) + "}")

    # Price ceiling
    parts.append(f"price:[..{MAX_BUY_PRICE}]")

    # Condition any
    # leave open

    return ",".join(parts)


def is_probably_lot(title: str) -> bool:
    t = (title or "").lower()
    for kw in NEGATIVE_KEYWORDS:
        if kw in t:
            return True
    return False


def browse_search(token: str, query: str, limit: int = 50, offset: int = 0):
    url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": EBAY_MARKETPLACE_ID,
        "Content-Type": "application/json",
    }

    params = {
        "q": query,
        "limit": str(limit),
        "offset": str(offset),
        "filter": build_filter_string(),
        "sort": "newlyListed",
    }

    r = requests.get(url, headers=headers, params=params, timeout=30)
    log(f"BROWSE STATUS: {r.status_code}")
    r.raise_for_status()
    return r.json()


def safe_float(x):
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def money_from_item(item):
    price = None
    ship = 0.0

    if isinstance(item, dict):
        p = item.get("price")
        if isinstance(p, dict):
            price = safe_float(p.get("value"))
        s = item.get("shippingOptions")
        if isinstance(s, list) and s:
            # take the cheapest shipping option if present
            vals = []
            for opt in s:
                if isinstance(opt, dict):
                    sc = opt.get("shippingCost")
                    if isinstance(sc, dict):
                        v = safe_float(sc.get("value"))
                        if v is not None:
                            vals.append(v)
            if vals:
                ship = min(vals)

    return price, ship


def listing_type_from_item(item):
    opts = item.get("buyingOptions") or []
    if "AUCTION" in opts:
        return "auction"
    if "FIXED_PRICE" in opts:
        return "fixed_price"
    return "unknown"


def sold_comps_median_usd(title: str):
    """
    Uses eBay Finding API findCompletedItems to estimate a median sold price.
    Requires EBAY_APP_ID.
    Returns float or None.
    """
    if not EBAY_APP_ID:
        return None

    endpoint = "https://svcs.ebay.com/services/search/FindingService/v1"
    params = {
        "OPERATION-NAME": "findCompletedItems",
        "SERVICE-VERSION": "1.13.0",
        "SECURITY-APPNAME": EBAY_APP_ID,
        "RESPONSE-DATA-FORMAT": "JSON",
        "REST-PAYLOAD": "true",
        "keywords": title,
        "paginationInput.entriesPerPage": "25",
        "itemFilter(0).name": "SoldItemsOnly",
        "itemFilter(0).value": "true",
        "itemFilter(1).name": "ListingType",
        "itemFilter(1).value": "All",
        "itemFilter(2).name": "Currency",
        "itemFilter(2).value": "USD",
    }

    try:
        r = requests.get(endpoint, params=params, timeout=30)
        if r.status_code != 200:
            return None
        data = r.json()
        resp = data.get("findCompletedItemsResponse")
        if not resp or not isinstance(resp, list):
            return None
        resp0 = resp[0]
        sr = resp0.get("searchResult")
        if not sr or not isinstance(sr, list):
            return None
        items = sr[0].get("item")
        if not items or not isinstance(items, list):
            return None

        sold_prices = []
        for it in items:
            ss = it.get("sellingStatus")
            if not ss or not isinstance(ss, list):
                continue
            cp = ss[0].get("currentPrice")
            if not cp or not isinstance(cp, list):
                continue
            v = safe_float(cp[0].get("__value__"))
            if v is not None:
                sold_prices.append(v)

        if not sold_prices:
            return None

        sold_prices.sort()
        mid = len(sold_prices) // 2
        if len(sold_prices) % 2 == 1:
            return float(sold_prices[mid])
        return float((sold_prices[mid - 1] + sold_prices[mid]) / 2.0)
    except Exception:
        return None


def calc_profit_and_score(buy_price: float, ship: float, sold_est: float):
    """
    Simple model:
    Total cost = buy + ship
    Fees estimate = 13.25% of sold + 0.30
    Profit = sold - fees - total_cost
    ROI = profit / total_cost
    Score = profit + (roi * 100)
    """
    if buy_price is None or sold_est is None:
        return None, None, None

    total_cost = max(0.0, float(buy_price) + float(ship or 0.0))
    if total_cost <= 0:
        return None, None, None

    fee = (0.1325 * sold_est) + 0.30
    profit = sold_est - fee - total_cost
    roi = profit / total_cost

    score = profit + (roi * 100.0)
    return profit, roi, score


def mark_all_inactive(cur):
    cur.execute("UPDATE deals SET active = FALSE;")


def upsert_deal(cur, d):
    cur.execute(
        """
        INSERT INTO deals (
            item_id, title, item_url, sold_url, buy_price, buy_shipping,
            est_profit, roi, score, listing_type,
            created_at, last_seen_at, active
        )
        VALUES (
            %(item_id)s, %(title)s, %(item_url)s, %(sold_url)s, %(buy_price)s, %(buy_shipping)s,
            %(est_profit)s, %(roi)s, %(score)s, %(listing_type)s,
            NOW(), NOW(), TRUE
        )
        ON CONFLICT (item_id) DO UPDATE SET
            title = EXCLUDED.title,
            item_url = EXCLUDED.item_url,
            sold_url = EXCLUDED.sold_url,
            buy_price = EXCLUDED.buy_price,
            buy_shipping = EXCLUDED.buy_shipping,
            est_profit = EXCLUDED.est_profit,
            roi = EXCLUDED.roi,
            score = EXCLUDED.score,
            listing_type = EXCLUDED.listing_type,
            last_seen_at = NOW(),
            active = TRUE
        """,
        d,
    )


def build_sold_url(title: str):
    # Completed sold search link
    q = quote_plus(title or "")
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_Complete=1&LH_Sold=1"


def run():
    log(f"SCANNER VERSION: {SCANNER_VERSION}")

    init_db()

    token = get_oauth_token()
    log("token ok")

    total_seen = 0
    total_kept = 0
    total_inserted = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Remove sold and expired items automatically by marking all inactive,
            # then only reactivate what we see this run
            mark_all_inactive(cur)
            conn.commit()

            for q in SEARCH_QUERIES:
                log(f"query: {q}")

                data = browse_search(token, q, limit=MAX_ITEMS_PER_QUERY, offset=0)

                items = data.get("itemSummaries") or []
                if not isinstance(items, list):
                    log("browse_search returned unexpected object")
                    continue

                log(f"items returned: {len(items)}")
                total_seen += len(items)

                for item in items:
                    if not isinstance(item, dict):
                        continue

                    item_id = item.get("itemId")
                    title = item.get("title") or ""
                    item_url = item.get("itemWebUrl") or ""

                    if not item_id or not title or not item_url:
                        continue

                    # Singles only
                    if is_probably_lot(title):
                        continue

                    buy_price, ship = money_from_item(item)
                    if buy_price is None:
                        continue

                    if buy_price > MAX_BUY_PRICE:
                        continue

                    listing_type = listing_type_from_item(item)

                    # Sold comps estimate
                    sold_est = sold_comps_median_usd(title)

                    est_profit, roi, score = calc_profit_and_score(buy_price, ship, sold_est)

                    # Filter to great upside
                    if est_profit is None or est_profit < MIN_EST_PROFIT:
                        continue

                    sold_url = build_sold_url(title)

                    d = {
                        "item_id": str(item_id),
                        "title": title,
                        "item_url": item_url,
                        "sold_url": sold_url,
                        "buy_price": float(buy_price),
                        "buy_shipping": float(ship or 0.0),
                        "est_profit": float(est_profit),
                        "roi": float(roi),
                        "score": float(score),
                        "listing_type": listing_type,
                    }

                    upsert_deal(cur, d)
                    total_inserted += 1
                    total_kept += 1

                    time.sleep(PAUSE_BETWEEN_CALLS_SEC)

            conn.commit()

    log(f"total_seen: {total_seen}")
    log(f"total_kept: {total_kept}")
    log(f"total_inserted: {total_inserted}")


if __name__ == "__main__":
    run()
