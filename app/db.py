import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

from app.db import init_db, get_conn
from app.ebay_auth import get_app_token
from app.ebay_browse import browse_search

SCANNER_VERSION = "PROFIT_25_OR_10_CHEAP_V2"

# Profit rules
MIN_PROFIT_MAIN = 25.0
MIN_PROFIT_CHEAP = 10.0

# What counts as cheap for the $10 profit rule
CHEAP_MAX_BUY_PRICE = 60.0
CHEAP_MAX_BUY_IN = 75.0

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


def is_cheap_candidate(buy_price: float, buy_shipping: float) -> bool:
    bp = _to_float(buy_price, 0.0)
    bi = buy_in(bp, _to_float(buy_shipping, 0.0))
    if bp <= 0:
        return False
    if bp > CHEAP_MAX_BUY_PRICE:
        return False
    if bi > CHEAP_MAX_BUY_IN:
        return False
    return True


def qualifies(est_profit: float, buy_price: float, buy_shipping: float) -> Tuple[bool, bool]:
    """
    Returns (keep, cheap_flag)
    """
    p = _to_float(est_profit, 0.0)

    if p >= MIN_PROFIT_MAIN:
        return True, False

    cheap = is_cheap_candidate(buy_price, buy_shipping)
    if cheap and p >= MIN_PROFIT_CHEAP:
        return True, True

    return False, False


def score_from(est_profit: float, roi: Optional[float], cheap: bool) -> float:
    p = _to_float(est_profit, 0.0)
    r = _to_float(roi, 0.0)

    base = p + (r * 10.0)

    if cheap:
        base += 5.0

    return base


def run() -> None:
    print(f"SCANNER VERSION: {SCANNER_VERSION}")

    init_db()
    token = get_app_token()
    print("SCANNER: token ok")

    # These are designed to actually surface cheaper stuff,
    # then we filter by estimated profit >= 10 (when cheap).
    cheap_queries = [
        "sports card lot",
        "rookie card lot",
        "mixed sports card lot",
        "raw rookie card",
        "ungraded rookie card",
        "refractor rookie card",
        "prizm rookie card",
        "optic holo rookie",
        "mosaic silver rookie",
        "select silver rookie",
        "topps chrome refractor rookie",
        "bowman chrome refractor",
        "autograph card",
        "patch card",
        "numbered card /99",
        "numbered card /50",
        "short print SSP",
        "case hit card",
        "downtown card",
        "kaboom card",
        "genesis card",
    ]

    # Higher end graded and chase queries
    core_queries = [
        "PSA graded card",
        "BGS graded card",
        "SGC graded card",
        "rookie PSA card",
        "autograph PSA card",
        "patch PSA card",
    ]

    queries = cheap_queries + core_queries

    total_seen = 0
    total_kept = 0
    total_cheap_kept = 0
    total_upserted = 0

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

            total_seen += 1

            # If the API is returning only pricey results for a query,
            # this filter forces us to keep looking for cheaper ones.
            # It also prevents the "cheap" section from being dominated by $500 listings.
            if q in cheap_queries:
                if buy_price > CHEAP_MAX_BUY_PRICE:
                    continue
                if buy_in(buy_price, buy_shipping) > CHEAP_MAX_BUY_IN:
                    continue

            est_profit = expected_profit(buy_price, buy_shipping)
            roi = roi_from_profit(est_profit, buy_price, buy_shipping)
            ltype = _listing_type(item)

            keep, cheap_flag = qualifies(est_profit, buy_price, buy_shipping)
            if not keep:
                continue

            if cheap_flag:
                total_cheap_kept += 1

            score = score_from(est_profit, roi, cheap_flag)
            total_kept += 1

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
                    total_upserted += 1

    print(f"SCANNER: total_seen: {total_seen}")
    print(f"SCANNER: total_kept: {total_kept}")
    print(f"SCANNER: total_cheap_kept: {total_cheap_kept}")
    print(f"SCANNER: total_upserted: {total_upserted}")


if __name__ == "__main__":
    run()
