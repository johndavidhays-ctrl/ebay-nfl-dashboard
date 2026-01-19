import math
import urllib.parse
from typing import Any, Dict, Optional, Tuple

from app.db import init_db, get_conn
from app.ebay_auth import get_app_token
from app.ebay_browse import browse_search


SCANNER_VERSION = "PROFIT_20_CHEAP_UPSIDE_FINAL"

# Minimum estimated profit required
MIN_PROFIT = 20.0

# Cost assumptions
SELL_FEE_RATE = 0.15
OUTBOUND_SHIP_SUPPLIES = 6.50
BUY_TAX_RATE = 0.06


def sold_url(title: str) -> str:
    q = urllib.parse.quote(title)
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_Sold=1&LH_Complete=1"


# Aggressive multipliers for cheap cards
def resale_multiplier(buy_price: float) -> float:
    if buy_price < 15:
        return 3.5
    if buy_price < 25:
        return 3.0
    if buy_price < 50:
        return 2.5
    if buy_price < 100:
        return 2.0
    return 1.45


def expected_profit(buy_price: float, buy_shipping: float) -> Tuple[float, float]:
    mult = resale_multiplier(buy_price)
    resale_estimate = buy_price * mult

    resale_net = resale_estimate * (1 - SELL_FEE_RATE) - OUTBOUND_SHIP_SUPPLIES
    buy_in = buy_price * (1 + BUY_TAX_RATE) + buy_shipping

    profit = resale_net - buy_in
    roi = profit / buy_in if buy_in > 0 else 0.0

    return profit, roi


def score_deal(est_profit: float, roi: float, buy_price: float, listing_type: str) -> float:
    # This scoring heavily favors cheap, high ROI cards
    score = 0.0
    score += max(0.0, roi) * 120.0
    score += est_profit * 0.6

    # Cheap price boost
    if buy_price > 0:
        score += 80.0 / (1.0 + buy_price)

    # Auction bonus
    if listing_type == "AUCTION":
        score += 4.0

    return round(score, 4)


def _as_json(obj: Any) -> Optional[Dict[str, Any]]:
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "json"):
        try:
            j = obj.json()
            if isinstance(j, dict):
                return j
        except Exception:
            return None
    return None


def _get_price(item: Dict[str, Any]) -> Optional[float]:
    price = item.get("price") or {}
    v = price.get("value")
    try:
        return float(v)
    except Exception:
        return None


def _get_shipping(item: Dict[str, Any]) -> float:
    opts = item.get("shippingOptions") or []
    if isinstance(opts, list) and opts:
        cost = (opts[0].get("shippingCost") or {}).get("value")
        try:
            return float(cost)
        except Exception:
            pass
    return 0.0


def _listing_type(item: Dict[str, Any]) -> str:
    buying = item.get("buyingOptions") or []
    s = {str(x).upper() for x in buying}
    if "AUCTION" in s:
        return "AUCTION"
    if "FIXED_PRICE" in s:
        return "FIXED_PRICE"
    if "BEST_OFFER" in s:
        return "BEST_OFFER"
    return "UNKNOWN"


def process_query(token: str, query: str, max_price: Optional[float]) -> int:
    raw = browse_search(token, query)
    data = _as_json(raw)
    if not data:
        return 0

    items = data.get("itemSummaries") or []
    inserted = 0

    for item in items:
        item_id = item.get("itemId")
        title = item.get("title")
        item_url = item.get("itemWebUrl")

        if not item_id or not title or not item_url:
            continue

        buy_price = _get_price(item)
        if buy_price is None:
            continue

        if max_price is not None and buy_price > max_price:
            continue

        buy_shipping = _get_shipping(item)
        listing_type = _listing_type(item)

        est_profit, roi = expected_profit(buy_price, buy_shipping)
        if est_profit < MIN_PROFIT:
            continue

        score = score_deal(est_profit, roi, buy_price, listing_type)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO deals
                      (item_id, title, item_url, sold_url,
                       buy_price, buy_shipping,
                       est_profit, roi, score, listing_type)
                    VALUES
                      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                        item_id,
                        title,
                        item_url,
                        sold_url(title),
                        float(buy_price),
                        float(buy_shipping),
                        float(est_profit),
                        float(roi),
                        float(score),
                        listing_type,
                    ),
                )
            conn.commit()

        inserted += 1

    return inserted


def run() -> None:
    print(f"SCANNER VERSION: {SCANNER_VERSION}")

    init_db()
    token = get_app_token()
    print("SCANNER: token ok")

    # Cheap upside focused searches
    cheap_queries = [
        "raw rookie card",
        "rookie card autograph",
        "numbered card /99",
        "numbered card /25",
        "case hit card",
        "prizm rookie",
        "optic rookie",
    ]

    # Higher priced slabs (kept but not prioritized)
    slab_queries = [
        "PSA graded card",
        "BGS graded card",
        "SGC graded card",
    ]

    total_inserted = 0

    print("SCANNER: cheap upside mode")
    for q in cheap_queries:
        print(f"SCANNER QUERY: {q}")
        total_inserted += process_query(token, q, max_price=80.0)

    print("SCANNER: slab mode")
    for q in slab_queries:
        print(f"SCANNER QUERY: {q}")
        total_inserted += process_query(token, q, max_price=None)

    print(f"SCANNER DONE. TOTAL INSERTED: {total_inserted}")


if __name__ == "__main__":
    run()
