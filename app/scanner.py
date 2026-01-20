import urllib.parse
import re
from typing import Any, Dict, List, Optional, Tuple

from app.db import init_db, get_conn
from app.ebay_auth import get_app_token
from app.ebay_browse import browse_search

SCANNER_VERSION = "SLEEPER_HUNT_UNDER_1000_MIN_PROFIT_10_MIN_BUY_10"

# Hard price rules
MIN_BUY_PRICE = 10.0
MAX_BUY_PRICE = 1000.0

# Minimum profit
MIN_PROFIT = 10.0

# Assumptions for fees and shipping
EBAY_FEE_RATE = 0.13

# How many results per query to inspect
MAX_ITEMS_PER_QUERY = 50

# Queries tuned for under the radar listings and lots
SEARCH_QUERIES = [
    "sports cards lot",
    "trading cards lot",
    "rookie card lot",
    "football cards lot",
    "basketball cards lot",
    "baseball cards lot",
    "vintage sports cards",
    "old sports cards",
    "sports card collection",
    "binder sports cards",
    "shoebox sports cards",
    "estate sale sports cards",
    "found in storage sports cards",
    "attic find sports cards",
    "misc sports cards",
    "cards from collection",
    "sports card lot no reserve",
    "trading card collection",
    "sports cards binder",
]

# Words that hint the card may be better than the seller realizes
VALUE_HINTS = {
    "refractor": 40,
    "holo": 25,
    "silver": 25,
    "chrome": 25,
    "rookie": 20,
    "rc": 20,
    "auto": 60,
    "autograph": 60,
    "patch": 50,
    "jersey": 35,
    "numbered": 40,
    "parallel": 25,
    "variation": 35,
    "sp": 20,
    "short print": 35,
    "insert": 12,
}

# Words that indicate a vague or sloppy listing
VAGUE_HINTS = [
    "lot", "cards", "collection", "binder", "shoebox", "misc", "vintage", "old",
    "estate", "storage", "found", "attic", "random", "assorted", "bulk"
]

# Small list of common misspelling patterns to catch sloppy titles
MISSPELL_BONUS_PATTERNS = [
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


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def listing_type_from_item(item: Dict[str, Any]) -> str:
    opts = item.get("buyingOptions") or []
    if "AUCTION" in opts:
        return "AUCTION"
    if "FIXED_PRICE" in opts:
        return "BIN"
    if "BEST_OFFER" in opts:
        return "BEST_OFFER"
    return "UNKNOWN"


def extract_price(item: Dict[str, Any]) -> float:
    return safe_float((item.get("price") or {}).get("value"), 0.0)


def extract_shipping(item: Dict[str, Any]) -> float:
    shipping = 0.0

    ship_opts = item.get("shippingOptions") or []
    if ship_opts:
        shipping = safe_float(((ship_opts[0] or {}).get("shippingCost") or {}).get("value"), 0.0)

    if shipping == 0.0:
        shipping = safe_float((item.get("shippingCost") or {}).get("value"), 0.0)

    return shipping


def estimate_value_from_title(title: str) -> float:
    t = f" {title.lower()} "
    base = 18.0

    bonus = 0.0
    for k, v in VALUE_HINTS.items():
        if k in t:
            bonus += float(v)

    if any(v in t for v in VAGUE_HINTS):
        bonus *= 1.15

    year_match = re.search(r"\b(19\d{2}|20\d{2})\b", t)
    if year_match:
        bonus += 8.0

    misspell_bonus = 0.0
    for pat in MISSPELL_BONUS_PATTERNS:
        if re.search(pat, t):
            misspell_bonus += 6.0

    return base + bonus + misspell_bonus


def estimate_profit(price: float, shipping: float, estimated_sale: float) -> float:
    total_cost = price + shipping
    ebay_fee = estimated_sale * EBAY_FEE_RATE
    return estimated_sale - total_cost - ebay_fee


def compute_roi(profit: float, cost: float) -> float:
    if cost <= 0:
        return 0.0
    return profit / cost


def compute_score(misprice: float, profit: float, roi: float, listing_type: str, title: str) -> float:
    score = 0.0

    score += misprice * 1.2
    score += profit * 2.0
    score += roi * 25.0

    if listing_type == "AUCTION":
        score += 18.0

    t = title.lower()
    if any(v in t for v in VAGUE_HINTS):
        score += 10.0

    for pat in MISSPELL_BONUS_PATTERNS:
        if re.search(pat, f" {t} "):
            score += 6.0

    return round(score, 2)


def run():
    print(f"SCANNER VERSION: {SCANNER_VERSION}")

    init_db()
    token = get_app_token()

    total_seen = 0
    total_kept = 0
    total_upserted = 0

    for query in SEARCH_QUERIES:
        print(f"SCANNER: query: {query}")

        data = browse_search(token, query)
        if not isinstance(data, dict):
            print("SCANNER: browse_search returned unexpected type")
            continue

        items = data.get("itemSummaries", [])
        if not isinstance(items, list):
            items = []

        if len(items) > MAX_ITEMS_PER_QUERY:
            items = items[:MAX_ITEMS_PER_QUERY]

        print(f"SCANNER: items returned: {len(items)}")

        for item in items:
            total_seen += 1

            item_id = item.get("itemId")
            title = item.get("title") or ""
            item_url = item.get("itemWebUrl")

            if not item_id or not title or not item_url:
                continue

            price = extract_price(item)
            shipping = extract_shipping(item)
            listing_type = listing_type_from_item(item)

            # Hard cap: nothing over $1000
            if price > MAX_BUY_PRICE:
                continue

            # Minimum buy price so we are not looking at junk
            if price < MIN_BUY_PRICE:
                continue

            est_value = estimate_value_from_title(title)
            profit = estimate_profit(price, shipping, est_value)
            cost = price + shipping
            roi = compute_roi(profit, cost)

            if profit < MIN_PROFIT:
                continue

            misprice = est_value - cost
            score = compute_score(misprice, profit, roi, listing_type, title)

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
                            listing_type,
                        ),
                    )
                    conn.commit()
                    total_upserted += 1
                    total_kept += 1

    print(f"SCANNER: total_seen: {total_seen}")
    print(f"SCANNER: total_kept: {total_kept}")
    print(f"SCANNER: total_upserted: {total_upserted}")


if __name__ == "__main__":
    run()
