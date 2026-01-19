# app/scanner.py

import urllib.parse
from app.db import init_db, get_conn
from app.ebay_auth import get_app_token
from app.ebay_browse import browse_search

SCANNER_VERSION = "PROFIT_25_ALL_SPORTS_STABLE"

MIN_PROFIT = 25.0
SELL_FEE_RATE = 0.15
OUTBOUND_SHIP_SUPPLIES = 6.50
BUY_TAX_RATE = 0.06
MAX_ITEMS_PER_QUERY = 50


def sold_url(title: str) -> str:
    q = urllib.parse.quote(title)
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_Sold=1&LH_Complete=1"


def expected_profit(buy_price: float, shipping: float) -> float:
    resale_estimate = buy_price * 1.45
    resale_net = resale_estimate * (1 - SELL_FEE_RATE) - OUTBOUND_SHIP_SUPPLIES
    buy_in = buy_price * (1 + BUY_TAX_RATE) + shipping
    return resale_net - buy_in


def normalize_browse_response(data):
    if not isinstance(data, dict):
        return []
    items = data.get("itemSummaries")
    if not isinstance(items, list):
        return []
    return items


def run():
    print("SCANNER VERSION:", SCANNER_VERSION)

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
        print("SCANNER: query:", q)

        try:
            data = browse_search(
                token=token,
                query=q,
                limit=MAX_ITEMS_PER_QUERY,
                include_auctions=True,
            )
        except Exception as e:
            print("SCANNER: browse error:", e)
            continue

        items = normalize_browse_response(data)
        print("SCANNER: items returned:", len(items))

        for item in items:
            total_seen += 1

            try:
                price = float(item["price"]["value"])
                shipping = float(
                    item.get("shippingOptions", [{}])[0]
                    .get("shippingCost", {})
                    .get("value", 0)
                )
            except Exception:
                continue

            profit = expected_profit(price, shipping)

            if profit < MIN_PROFIT:
                continue

            total_profitable += 1

            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO deals
                            (item_id, title, item_url, sold_url, buy_price, buy_shipping)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (item_id) DO NOTHING
                            """,
                            (
                                item["itemId"],
                                item["title"],
                                item["itemWebUrl"],
                                sold_url(item["title"]),
                                price,
                                shipping,
                            ),
                        )
                        if cur.rowcount > 0:
                            total_inserted += 1
                    conn.commit()
            except Exception as e:
                print("DB insert failed:", e)

    print("SCANNER: total_seen:", total_seen)
    print("SCANNER: total_profitable:", total_profitable)
    print("SCANNER: total_inserted:", total_inserted)


if __name__ == "__main__":
    run()
