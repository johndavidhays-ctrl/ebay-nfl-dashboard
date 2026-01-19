import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

from app.db import init_db, get_conn
from app.ebay_auth import get_app_token
from app.ebay_browse import browse_search

SCANNER_VERSION = "PROFIT_25_PLUS_DOUBLE_10_ALL_CARDS"

# Profit rules
MIN_PROFIT = 25.0
MIN_BUY_IN_FOR_DOUBLE = 10.0

# Estimation assumptions
SELL_FEE_RATE = 0.15
OUTBOUND_SHIP_SUPPLIES = 6.50
BUY_TAX_RATE = 0.06

MAX_ITEMS_PER_QUERY = 50


def sold_url(title: str) -> str:
    q = urllib.parse.quote(title)
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_Sold=1&LH_Complete=1"


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _safe_json(obj: Any) -> Optional[Dict[str, Any]]:
    if obj is None:
        return None

    if isinstance(obj, dict):
        return obj

    if hasattr(obj, "json") and callable(getattr(obj, "json")):
        try:
            return obj.json()
        except Exception:
            return None

    return None


def _extract_shipping(item: Dict[str, Any]) -> float:
    ship = 0.0

    shipping_options = item.get("shippingOptions")
    if isinstance(shipping_options, list) and shipping_options:
        first = shipping_options[0] or {}
        cost = (first.get("shippingCost") or {}).get("value")
        ship = _to_float(cost, 0.0)

    if ship == 0.0:
        ship_cost = (item.get("shippingCost") or {}).get("value")
        ship = _to_float(ship_cost, 0.0)

    return ship


def _extract_price(item: Dict[str, Any]) -> float:
    price = item.get("price") or {}
    return _to_float(price.get("value"), 0.0)


def _listing_type(item: Dict[str, Any]) -> str:
    opts = item.get("buyingOptions")
    if isinstance(opts, list) and opts:
        if "AUCTION" in opts:
            return "auction"
        if "FIXED_PRICE" in opts:
            return "fixed"
    return "unknown"


def buy_in(buy_price: float, buy_shipping: float) -> float:
    return _to_float(buy_price, 0.0) * (1.0 + BUY_TAX_RATE) + _to_float(buy_shipping, 0.0)


def expected_profit(buy_price: float, buy_shipping: float) -> float:
    """
    Conservative heuristic.
    We under estimate resale by using a multiplier, then subtract fees and shipping.
    """
    bp = _to_float(buy_price, 0.0)
    bs = _to_float(buy_shipping, 0.0)

    resale_estimate = bp * 1.45
    resale_net = resale_estimate * (1.0 - SELL_FEE_RATE) - OUTBOUND_SHIP_SUPPLIES
    return resale_net - buy_in(bp, bs)


def roi_from_profit(est_profit: float, buy_price: float, buy_shipping: float) -> Optional[float]:
    bi = buy_in(buy_price, buy_shipping)
    if bi <= 0:
        return None
    return _to_float(est_profit, 0.0) / bi


def qualifies(est_profit: float, buy_price: float, buy_shipping: float) -> bool:
    p = _to_float(est_profit, 0.0)
    bi = buy_in(buy_price, buy_shipping)

    if p >= MIN_PROFIT:
        return True

    if bi >= MIN_BUY_IN_FOR_DOUBLE and p >= bi:
        return True

    return False


def score_from(est_profit: float, roi: Optional[float]) -> float:
    """
    Simple ranking score.
    Profit matters most, ROI boosts.
    """
    p = _to_float(est_profit, 0.0)
    r = _to_float(roi, 0.0)
    return p + (r * 10.0)


def _normalize_items(data: Any) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    payload = _safe_json(data)
    if not payload:
        return None, []

    items = payload.get("itemSummaries", [])
    if not isinstance(items, list):
        return payload, []

    out: List[Dict[str, Any]] = []
    for it in items:
        if isinstance(it, dict):
            out.append(it)
    return payload, out


def run() -> None:
    print(f"SCANNER VERSION: {SCANNER_VERSION}")

    init_db()
    token = get_app_token()
    print("SCANNER: token ok")

    queries = [
        "PSA graded card",
        "BGS graded card",
        "SGC graded card",
        "rookie PSA card",
        "autograph PSA card",
        "patch PSA card",
    ]

    total_seen = 0
    total_profitable = 0
    total_inserted = 0

    for q in queries:
        print(f"SCANNER: query: {q}")

        result = browse_search(token, q)

        payload, items = _normalize_items(result)
        if payload is None:
            print("SCANNER: invalid response type")
            continue

        status = payload.get("httpStatusCode") or payload.get("status")
        if status is not None:
            print(f"BROWSE STATUS: {status}")

        if len(items) > MAX_ITEMS_PER_QUERY:
            items = items[:MAX_ITEMS_PER_QUERY]

        print(f"SCANNER: items returned: {len(items)}")

        for item in items:
            item_id = item.get("itemId")
            title = item.get("title")
            item_url = item.get("itemWebUrl")

            if not item_id or not title or not item_url:
                continue

            buy_price = _extract_price(item)
            buy_shipping = _extract_shipping(item)

            est_profit = expected_profit(buy_price, buy_shipping)
            roi = roi_from_profit(est_profit, buy_price, buy_shipping)
            score = score_from(est_profit, roi)
            ltype = _listing_type(item)

            total_seen += 1

            if not qualifies(est_profit, buy_price, buy_shipping):
                continue

            total_profitable += 1

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO deals
                        (item_id, title, item_url, sold_url, buy_price, buy_shipping,
                         est_profit, roi, score, listing_type)
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
                            str(title),
                            str(item_url),
                            sold_url(str(title)),
                            float(buy_price),
                            float(buy_shipping),
                            float(est_profit),
                            float(roi) if roi is not None else None,
                            float(score),
                            str(ltype),
                        ),
                    )
                    conn.commit()
                    total_inserted += 1

    print(f"SCANNER: total_seen: {total_seen}")
    print(f"SCANNER: total_profitable: {total_profitable}")
    print(f"SCANNER: total_inserted: {total_inserted}")


if __name__ == "__main__":
    run()
