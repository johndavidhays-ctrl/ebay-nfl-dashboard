import urllib.parse
import re
from app.db import init_db, get_conn
from app.ebay_auth import get_app_token
from app.ebay_browse import browse_search

MIN_BUY_PRICE = 5.0
MAX_BUY_PRICE = 150.0

MIN_PROFIT = 10.0
MIN_ROI = 1.0

EBAY_FEE_RATE = 0.13

SEARCH_QUERIES = [
    "sports cards lot",
    "rookie card lot",
    "football cards lot",
    "basketball cards lot",
    "baseball cards lot",
    "vintage sports cards",
    "old sports cards",
    "card collection",
    "binder sports cards",
    "shoebox sports cards",
    "estate sale sports cards",
    "found in storage sports cards",
    "attic find sports cards",
    "misc sports cards",
    "sports card lot no reserve",
    "trading cards lot"
]

VALUE_HINTS = {
    "refractor": 40,
    "holo": 25,
    "silver": 25,
    "chrome": 25,
    "prizm": 25,
    "optic": 18,
    "select": 18,
    "mosaic": 15,
    "rookie": 20,
    "rc": 20,
    "auto": 60,
    "autograph": 60,
    "patch": 50,
    "jersey": 35,
    "numbered": 40,
    "parallel": 25,
    "variation": 35,
    "sp": 25,
    "short print": 35,
}

VAGUE_HINTS = [
    "lot", "cards", "collection", "binder", "shoebox", "misc", "vintage", "old",
    "estate", "storage", "found", "attic"
]

def sold_url(title):
    q = urllib.parse.quote(title)
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_Sold=1&LH_Complete=1"

def safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def listing_type_from_item(item):
    opts = item.get("buyingOptions") or []
    if "AUCTION" in opts:
        return "AUCTION"
    if "FIXED_PRICE" in opts:
        return "BIN"
    return "UNKNOWN"

def estimate_value_from_title(title: str) -> float:
    t = title.lower()
    base = 18.0

    bonus = 0.0
    for k, v in VALUE_HINTS.items():
        if k in t:
            bonus += v

    is_vague = any(v in t for v in VAGUE_HINTS)
    if is_vague:
        bonus *= 1.15

    year_match = re.search(r"\b(19\d{2}|20\d{2})\b", t)
    if year_match:
        bonus += 8

    return base + bonus

def estimate_profit(price, shipping, estimated_sale):
    total_cost = price + shipping
    ebay_fee = estimated_sale * EBAY_FEE_RATE
    return estimated_sale - total_cost - ebay_fee

def compute_score(misprice, profit, roi, listing_type, title):
    score = 0.0

    score += misprice * 1.2
    score += profit * 2.0
    score += roi * 25.0

    if listing_type == "AUCTION":
        score += 20.0

    t = title.lower()
    if any(v in t for v in VAGUE_HINTS):
        score += 10.0

    return round(score, 2)

def run():
    init_db()
    token = get_app_token()

    total_seen = 0
    total_kept = 0
    total_inserted = 0

    for query in SEARCH_QUERIES:
        print(f"SCANNER: query: {query}")
        data = browse_search(token, query)

        if not isinstance(data, dict):
            print("SCANNER: browse_search returned unexpected object")
            continue

        items = data.get("itemSummaries", [])
        print(f"SCANNER: items returned: {len(items)}")

        for item in items:
            total_seen += 1

            title = item.get("title") or ""
            if not title:
                continue

            price = safe_float((item.get("price") or {}).get("value"), 0.0)

            shipping = 0.0
            ship_opts = item.get("shippingOptions") or []
            if ship_opts:
                shipping = safe_float(((ship_opts[0] or {}).get("shippingCost") or {}).get("value"), 0.0)

            if price < MIN_BUY_PRICE or price > MAX_BUY_PRICE:
                continue

            est_value = estimate_value_from_title(title)

            profit = estimate_profit(price, shipping, est_value)
            cost = price + shipping
            roi = profit / cost if cost > 0 else 0.0

            if profit < MIN_PROFIT:
                continue

            if roi < MIN_ROI:
                continue

            misprice = est_value - cost
            listing_type = listing_type_from_item(item)
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
                            item.get("itemId"),
                            title,
                            item.get("itemWebUrl"),
                            sold_url(title),
                            round(price, 2),
                            round(shipping, 2),
                            round(profit, 2),
                            round(roi, 2),
                            score,
                            listing_type
                        )
                    )
                    conn.commit()

            total_inserted += 1
            total_kept += 1

    print(f"SCANNER: total_seen: {total_seen}")
    print(f"SCANNER: total_kept: {total_kept}")
    print(f"SCANNER: total_inserted: {total_inserted}")

if __name__ == "__main__":
    run()
