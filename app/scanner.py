print("SCANNER VERSION: PROFIT_25_ALL_SPORTS_LIVE")

import urllib.parse
from app.db import init_db, get_conn
from app.ebay_auth import get_app_token
from app.ebay_browse import browse_search


# ---------- CONFIG ----------
MIN_PROFIT = 25.0
SELL_FEE_RATE = 0.15
OUTBOUND_SHIP_SUPPLIES = 6.50
BUY_TAX_RATE = 0.06
# ----------------------------


def sold_url(title: str) -> str:
    q = urllib.parse.quote(title)
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_Sold=1&LH_Complete=1"


def expected_profit(buy_price: float, buy_shipping: float) -> float:
    """
    Conservative heuristic profit estimate.
    We intentionally under-estimate resale price.
    """
    resale_estimate = buy_price * 1.45

    resale_net = resale_estimate * (1 - SELL_FEE_RATE) - OUTBOUND_SHIP_SUPPLIES
    buy_in = buy_price * (1 + BUY_TAX_RATE) + buy_shipping

    return resale_net - buy_in


def run():
    init_db()

    token = get_app_token()
    print("SCANNER: token ok")

    queries = [
        "(PSA OR BGS OR SGC OR CGC) (rookie OR prizm OR optic OR auto OR autograph)",
        "(PSA OR BGS OR SGC OR CGC) (downtown OR kaboom OR color blast OR genesis OR gold)",
        "(PSA OR BGS OR SGC OR CGC) (patch OR jersey OR rpa OR on-card)",
    ]

    total_seen = 0
    total_profitable = 0
    total_inserted = 0

    for q in queries:
        print("SCANNER: query:", q)

        data = browse_search(token, q)
        items = data.get("itemSummaries", []) or []
        print("SCANNER: items returned:", len(items))

        for item in items:
            total_seen += 1

            title = (item.get("title") or "").strip()
            item_id = item.get("itemId") or ""
            item_url = item.get("itemWebUrl") or ""

            price_val = ((item.get("price") or {}).get("value")) or 0
            buy_price = float(price_val)

            buy_shipping = 0.0
            ship = item.get("shippingOptions") or []
            if ship and isinstance(ship, list):
                cost = (ship[0].get("shippingCost") or {}).get("value")
                if cost is not None:
                    buy_shipping = float(cost)

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
                            item_url if item_url else sold_url(title),
                            sold_url(title),
                            buy_price,
                            buy_shipping,
                        ),
                    )
                    if cur.rowcount == 1:
                        total_inserted += 1

                conn.commit()

    print("SCANNER: total_seen:", total_seen)
    print("SCANNER: total_profitable:", total_profitable)
    print("SCANNER: total_inserted:", total_inserted)


if __name__ == "__main__":
    run()
