import urllib.parse
from decimal import Decimal

from app.db import init_db, get_conn
from app.ebay_auth import get_app_token
from app.ebay_browse import browse_search


SCANNER_VERSION = "PROFIT_25_ALL_SPORTS_WITH_SCORE_AND_AUCTIONS"

MIN_PROFIT = Decimal("25.00")

SELL_FEE_RATE = Decimal("0.15")
OUTBOUND_SHIP_SUPPLIES = Decimal("6.50")
BUY_TAX_RATE = Decimal("0.06")

MAX_ITEMS_PER_QUERY = 50


def sold_url(title: str) -> str:
    q = urllib.parse.quote(title)
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_Sold=1&LH_Complete=1"


def d(x) -> Decimal:
    if x is None:
        return Decimal("0")
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal("0")


def safe_get(obj, *path, default=None):
    cur = obj
    for key in path:
        if cur is None:
            return default
        if isinstance(cur, dict):
            cur = cur.get(key)
        elif isinstance(cur, list):
            try:
                cur = cur[key]
            except Exception:
                return default
        else:
            return default
    if cur is None:
        return default
    return cur


def normalize_browse_result(result):
    if isinstance(result, dict):
        return result
    if hasattr(result, "json"):
        try:
            return result.json()
        except Exception:
            return {}
    return {}


def parse_listing_type(item: dict) -> str:
    opts = safe_get(item, "buyingOptions", default=[])
    if isinstance(opts, list):
        if "AUCTION" in opts:
            return "auction"
        if "FIXED_PRICE" in opts:
            return "fixed"
        if "BEST_OFFER" in opts:
            return "best_offer"
    return ""


def parse_buy_price(item: dict) -> Decimal:
    price_val = safe_get(item, "price", "value")
    if price_val is not None:
        return d(price_val)

    bid_val = safe_get(item, "currentBidPrice", "value")
    if bid_val is not None:
        return d(bid_val)

    return Decimal("0")


def parse_shipping(item: dict) -> Decimal:
    ship_val = safe_get(item, "shippingOptions", 0, "shippingCost", "value")
    if ship_val is not None:
        return d(ship_val)

    ship_val = safe_get(item, "shippingOptions", 0, "cost", "value")
    if ship_val is not None:
        return d(ship_val)

    return Decimal("0")


def expected_profit(buy_price: Decimal, buy_shipping: Decimal) -> Decimal:
    """
    Conservative heuristic.
    We do not know true comps yet, so we estimate resale as a multiple of buy.
    """
    if buy_price <= 0:
        return Decimal("0")

    resale_estimate = buy_price * Decimal("1.45")

    resale_net = (resale_estimate * (Decimal("1.00") - SELL_FEE_RATE)) - OUTBOUND_SHIP_SUPPLIES
    buy_in = (buy_price * (Decimal("1.00") + BUY_TAX_RATE)) + buy_shipping

    return resale_net - buy_in


def compute_roi(profit: Decimal, buy_price: Decimal, buy_shipping: Decimal) -> Decimal:
    buy_in = (buy_price * (Decimal("1.00") + BUY_TAX_RATE)) + buy_shipping
    if buy_in <= 0:
        return Decimal("0")
    return profit / buy_in


def compute_score(profit: Decimal, roi: Decimal, listing_type: str) -> Decimal:
    """
    Simple score:
    profit matters most, roi matters next, auctions get a small boost.
    """
    score = profit + (roi * Decimal("100.0"))
    if listing_type == "auction":
        score += Decimal("5.0")
    return score


def upsert_deal(
    item_id: str,
    title: str,
    item_url: str,
    sold_comp_url: str,
    buy_price: Decimal,
    buy_shipping: Decimal,
    est_profit: Decimal,
    roi: Decimal,
    score: Decimal,
    listing_type: str,
) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO deals
                  (item_id,title,item_url,sold_url,buy_price,buy_shipping,est_profit,roi,score,listing_type)
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
                  listing_type = EXCLUDED.listing_type,
                  updated_at = now();
                """,
                (
                    item_id,
                    title,
                    item_url,
                    sold_comp_url,
                    float(buy_price),
                    float(buy_shipping),
                    float(est_profit),
                    float(roi),
                    float(score),
                    listing_type,
                ),
            )
            conn.commit()


def run():
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

        raw = browse_search(token, q)
        data = normalize_browse_result(raw)

        items = safe_get(data, "itemSummaries", default=[])
        if not isinstance(items, list):
            print("SCANNER: invalid response type")
            continue

        print(f"SCANNER: items returned: {len(items)}")

        for item in items[:MAX_ITEMS_PER_QUERY]:
            total_seen += 1

            item_id = safe_get(item, "itemId", default="")
            title = safe_get(item, "title", default="")
            item_url = safe_get(item, "itemWebUrl", default="")

            if not item_id or not title or not item_url:
                continue

            listing_type = parse_listing_type(item)
            buy_price = parse_buy_price(item)
            buy_shipping = parse_shipping(item)

            profit = expected_profit(buy_price, buy_shipping)
            roi = compute_roi(profit, buy_price, buy_shipping)
            score = compute_score(profit, roi, listing_type)

            if profit < MIN_PROFIT:
                continue

            total_profitable += 1

            upsert_deal(
                item_id=item_id,
                title=title,
                item_url=item_url,
                sold_comp_url=sold_url(title),
                buy_price=buy_price,
                buy_shipping=buy_shipping,
                est_profit=profit,
                roi=roi,
                score=score,
                listing_type=listing_type,
            )
            total_inserted += 1

    print(f"SCANNER: total_seen: {total_seen}")
    print(f"SCANNER: total_profitable: {total_profitable}")
    print(f"SCANNER: total_inserted: {total_inserted}")


if __name__ == "__main__":
    run()
