import urllib.parse
from typing import Any, Dict, List

from app.db import init_db, get_conn
from app.ebay_auth import get_app_token
from app.ebay_browse import browse_search


SCANNER_VERSION = "PROFIT_25_ALL_SPORTS_LIVE_V3"

MIN_PROFIT = 25.0

SELL_FEE_RATE = 0.15
OUTBOUND_SHIP_SUPPLIES = 6.50
BUY_TAX_RATE = 0.06

QUERIES = [
    '(PSA OR BGS OR SGC OR CGC) (rookie OR prizm OR optic OR auto OR autograph)',
    '(PSA OR BGS OR SGC OR CGC) (downtown OR kaboom OR "color blast" OR genesis OR gold)',
    '(PSA OR BGS OR SGC OR CGC) (patch OR jersey OR rpa OR "on card" OR "on-card")',
]


def sold_url(title: str) -> str:
    q = urllib.parse.quote(title)
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_Sold=1&LH_Complete=1"


def to_json(result: Any) -> Dict[str, Any]:
    if result is None:
        return {}

    if isinstance(result, dict):
        return result

    if hasattr(result, "json"):
        try:
            data = result.json()
            if isinstance(data, dict):
                return data
            return {}
        except Exception:
            return {}

    return {}


def extract_items(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = data.get("itemSummaries")
    if isinstance(items, list):
        return items
    return []


def get_price(item: Dict[str, Any]) -> float:
    try:
        return float((item.get("price") or {}).get("value") or 0)
    except Exception:
        return 0.0


def get_shipping(item: Dict[str, Any]) -> float:
    try:
        opts = item.get("shippingOptions") or []
        if not isinstance(opts, list) or not opts:
            return 0.0
        first = opts[0] or {}
        cost = (first.get("shippingCost") or {}).get("value") or 0
        return float(cost)
    except Exception:
        return 0.0


def expected_profit(buy_price: float, buy_shipping: float) -> float:
    resale_estimate = buy_price * 1.45
    resale_net = (resale_estimate * (1 - SELL_FEE_RATE)) - OUTBOUND_SHIP_SUPPLIES
    buy_in = (buy_price * (1 + BUY_TAX_RATE)) + buy_shipping
    return resale_net - buy_in


def run() -> None:
    print(f"SCANNER VERSION: {SCANNER_VERSION}")

    init_db()

    token = get_app_token()
    print("SCANNER: token ok")

    total_seen = 0
    total_profitable = 0
    total_inserted = 0

    for q in QUERIES:
        print(f"SCANNER: query: {q}")

        result = browse_search(token, q)
        data = to_json(result)

        items = extract_items(data)
        print(f"SCANNER: items returned: {len(items)}")

        for item in items:
            total_seen += 1

            item_id = item.get("itemId")
            title = item.get("title") or ""
            item_url = item.get("itemWebUrl") or ""

            if not item_id or not title or not item_url:
                continue

            buy_price = get_price(item)
            buy_shipping = get_shipping(item)

            profit = expected_profit(buy_price, buy_shipping)
            if profit < MIN_PROFIT:
                continue

            total_profitable += 1

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO deals
                        (item_id,title,item_url,sold_url,buy_price,buy_shipping)
                        VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (item_id) DO NOTHING
                        """,
                        (
                            item_id,
                            title,
                            item_url,
                            sold_url(title),
                            float(buy_price),
                            float(buy_shipping),
                        ),
                    )

                    if cur.rowcount and cur.rowcount > 0:
                        total_inserted += 1

                conn.commit()

    print(f"SCANNER: total_seen: {total_seen}")
    print(f"SCANNER: total_profitable: {total_profitable}")
    print(f"SCANNER: total_inserted: {total_inserted}")


if __name__ == "__main__":
    run()
