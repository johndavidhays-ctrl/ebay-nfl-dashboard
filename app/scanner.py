import urllib.parse
import re
from app.db import init_db, get_conn
from app.ebay_auth import get_app_token
from app.ebay_browse import browse_search

SCANNER_VERSION = "SINGLES_ONLY_SLEEPER_HUNT"

MIN_BUY_PRICE = 10.0
MAX_BUY_PRICE = 1000.0
MIN_PROFIT = 10.0

EBAY_FEE_RATE = 0.13
MAX_ITEMS_PER_QUERY = 50

# Search terms designed for singles, not lots
SEARCH_QUERIES = [
    "rookie card",
    "autograph card",
    "patch card",
    "jersey card",
    "numbered card",
    "refractor card",
    "silver card",
    "chrome card",
    "parallel card",
    "insert card",
    "variation card",
    "short print card",
    "sp card",
    "sports card",
]

# Hard exclude anything that looks like a lot
LOT_EXCLUDE_WORDS = [
    "lot", "lots", "collection", "binder", "shoebox", "bulk",
    "cards", "assorted", "misc", "set of", "complete set"
]

# Signals that imply higher value than seller may realize
VALUE_HINTS = {
    "refractor": 45,
    "silver": 30,
    "chrome": 30,
    "rookie": 25,
    "rc": 25,
    "auto": 70,
    "autograph": 70,
    "patch": 55,
    "jersey": 40,
    "numbered": 45,
    "parallel": 30,
    "variation": 40,
    "short print": 40,
    "sp": 25,
    "insert": 15,
}

# Misspelling patterns that indicate sloppy listings
MISSPELL_PATTERNS = [
    r"\srooky\s",
    r"\srooki\s",
    r"\sautographh\s",
    r"\srefractorr\s",
    r"\sprizim\s",
    r"\soptik\s",
]

def sold_url(title):
    q = urllib.parse.quote(title)
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_Sold=1&LH_Complete=1"

def safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def extract_price(item):
    return safe_float((item.get("price") or {}).get("value"), 0.0)

def extract_shipping(item):
    ship = 0.0
    ship_opts = item.get("shippingOptions") or []
    if ship_opts:
        ship = safe_float(((ship_opts[0] or {}).get("shippingCost") or {}).get("value"), 0.0)
    return ship

def listing_type(item):
    opts = item.get("buyingOptions") or []
    if "AUCTION" in opts:
        return "AUCTION"
    if "FIXED_PRICE" in opts:
        return "BIN"
    return "UNKNOWN"

def looks_like_lot(title: str) -> bool:
    t = title.lower()
    return any(w in t for w in LOT_EXCLUDE_WORDS)

def estimate_value_from_title(title: str) -> float:
    t = f" {title.lower()} "
    base = 20.0
    bonus = 0.0

    for k, v in VALUE_HINTS.items():
        if k in t:
            bonus += v

    year_match = re.search(r"\b(19\d{2}|20\d{2})\b", t)
    if year_match:
        bonus += 10.0

    for pat in MISSPELL_PATTERNS:
        if re.search(pat, t):
            bonus += 8.0

    return base + bonus

def estimate_profit(price, shipping, est_sale):
    total_cost = price + shipping
    ebay_fee = est_sale * EBAY_FEE_RATE
    return est_sale - total_cost - ebay_fee

def compute_score(misprice, profit, roi, ltype):
    score = 0.0
    score += misprice * 1.2
    score += profit * 2.0
    score += roi * 25.0
    if ltype == "AUCTION":
        score += 20.0
    return round(score, 2)

def run():
    print(f"SCANNER VERSION: {SCANNER_VERSION}")

    init_db()
    token = get_app_token()

    total_seen = 0
    total_kept = 0
    total_inserted = 0

    for query in SEARCH_QUERIES:
        print(f"SCANNER: query: {query}")
        data = browse_search(token, query)

        if not isinstance(data, dict):
            continue

        items = data.get("itemSummaries", [])
        if not isinstance(items, list):
            continue

        items = items[:MAX_ITEMS_PER_QUERY]
        print(f"SCANNER: items returned: {len(items)}")

        for item in items:
            total_seen += 1

            title = item.get("title") or ""
            if not title or looks_like_lot(title):
                continue

            price = extract_price(item)
            shipping = extract_shipping(item)

            if price < MIN_BUY_PRICE or price > MAX_BUY_PRICE:
                continue

            est_value = estimate_value_from_title(title)
            profit = estimate_profit(price, shipping, est_value)
            cost = price + shipping
            roi = profit / cost if cost > 0 else 0.0

            if profit < MIN_PROFIT:
                continue

            ltype = listing_type(item)
            misprice = est_value - cost
            score = compute_score(misprice, profit, roi, ltype)

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO deals (
                            item_id,
                            title,
                            item_url,
                            sold_url,
                            buy_price,
                            buy_shipping,
                            est_profit,
                            roi,
                            score,
                            listing_type
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (item_id) DO UPDATE SET
                            title = EXCLUDED.title,
                            item_url = EXCLUDED.item_url,
                            sold_url = EXCLUDED.sold_url,
                            buy_price = EXCLUDED.buy_price,
                            buy_shipping = EXCLUDED.buy_shipping,
                            est_profit = EXCLUDED.est_profit,
                            roi = EXCLUDED.roi,
                            score = EXCLUDED.score,
                            listing_type = EXCLUDED.listing_type
                        """,
                        (
                            item.get("itemId"),
                            title,
                            item.get("itemWebUrl"),
                            sold_url(title),
                            round(price, 2),
                            round(shipping, 2),
                            round(profit, 2),
                            round(roi, 2),
                            score,
                            ltype,
                        ),
                    )
                    conn.commit()

            total_inserted += 1
            total_kept += 1

    print(f"SCANNER: total_seen: {total_seen}")
    print(f"SCANNER: total_kept: {total_kept}")
    print(f"SCANNER: total_inserted: {total_inserted}")

if __name__ == "__main__":
    run()
