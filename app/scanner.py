import urllib.parse
import re
from app.db import init_db, get_conn
from app.ebay_auth import get_app_token
from app.ebay_browse import browse_search

SCANNER_VERSION = "SINGLES_ONLY_100_PROFIT_BIG_UPSIDE_UNDER_1000"

# Singles only, no lots
MIN_BUY_PRICE = 10.0
MAX_BUY_PRICE = 1000.0

# Only show big upside opportunities
MIN_PROFIT = 100.0

# Require estimated value to be at least 2x total cost
UPSIDE_MULTIPLE = 2.0

EBAY_FEE_RATE = 0.13
MAX_ITEMS_PER_QUERY = 50

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

LOT_EXCLUDE_WORDS = [
    "lot", "lots", "collection", "binder", "shoebox", "bulk",
    "assorted", "misc", "set of", "complete set"
]

VALUE_HINTS = {
    "refractor": 45,
    "holo": 25,
    "silver": 30,
    "chrome": 30,
    "rookie": 25,
    "rc": 25,
    "auto": 80,
    "autograph": 80,
    "patch": 60,
    "jersey": 45,
    "numbered": 55,
    "parallel": 35,
    "variation": 45,
    "short print": 45,
    "sp": 25,
    "insert": 20,
}

MISSPELL_PATTERNS = [
    r"\srooky\s",
    r"\srooki\s",
    r"\sautographh\s",
    r"\srefractorr\s",
    r"\sprizim\s",
    r"\soptik\s",
]


def sold_url(title: str) -> str:
    q = urllib.parse.quote(title)
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_Sold=1&LH_Complete=1"


def safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def extract_price(item) -> float:
    return safe_float((item.get("price") or {}).get("value"), 0.0)


def extract_shipping(item) -> float:
    ship = 0.0
    ship_opts = item.get("shippingOptions") or []
    if ship_opts:
        ship = safe_float(((ship_opts[0] or {}).get("shippingCost") or {}).get("value"), 0.0)
    return ship


def listing_type(item) -> str:
    opts = item.get("buyingOptions") or []
    if "AUCTION" in opts:
        return "AUCTION"
    if "FIXED_PRICE" in opts:
        return "BIN"
    if "BEST_OFFER" in opts:
        return "BEST_OFFER"
    return "UNKNOWN"


def looks_like_lot(title: str) -> bool:
    t = title.lower()
    return any(w in t for w in LOT_EXCLUDE_WORDS)


def estimate_value_from_title(title: str) -> float:
    t = f" {title.lower()} "
    base = 35.0
    bonus = 0.0

    for k, v in VALUE_HINTS.items():
        if k in t:
            bonus += float(v)

    year_match = re.search(r"\b(19\d{2}|20\d{2})\b", t)
    if year_match:
        bonus += 15.0

    for pat in MISSPELL_PATTERNS:
        if re.search(pat, t):
            bonus += 12.0

    return base + bonus


def estimate_profit(price: float, shipping: float, est_sale: float) -> float:
    total_cost = price + shipping
    ebay_fee = est_sale * EBAY_FEE_RATE
    return est_sale - total_cost - ebay_fee


def has_big_upside(est_value: float, total_cost: float) -> bool:
    if total_cost <= 0:
        return False
    return est_value >= total_cost * UPSIDE_MULTIPLE


def compute_score(misprice: float, profit: float, roi: float, ltype: str) -> float:
    score = 0.0
    score += misprice * 1.4
    score += profit * 2.2
    score += roi * 30.0

    if ltype == "AUCTION":
        score += 30.0

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

            item_id = item.get("itemId")
            title = item.get("title") or ""
            item_url = item.get("itemWebUrl")

            if not item_id or not title or not item_url:
                continue

            if looks_like_lot(title):
                continue

            price = extract_price(item)
            shipping = extract_shipping(item)

            if price < MIN_BUY_PRICE or price > MAX_BUY_PRICE:
                continue

            est_value = estimate_value_from_title(title)
            cost = price + shipping

            if not has_big_upside(est_value, cost):
                continue

            profit = estimate_profit(price, shipping, est_value)
            if profit < MIN_PROFIT:
                continue

            roi = profit / cost if cost > 0 else 0.0
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
                            str(item_id),
                            title,
                            str(item_url),
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
