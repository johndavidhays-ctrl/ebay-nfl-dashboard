import urllib.parse
from app.db import init_db, get_conn
from app.ebay_auth import get_app_token
from app.ebay_browse import browse_search

# ---------------- CONFIG ----------------

MIN_PROFIT = 25.0
SELL_FEE_RATE = 0.15
OUTBOUND_SHIP_SUPPLIES = 6.50
BUY_TAX_RATE = 0.06

SEARCH_TERMS = [
    "PSA rookie card",
    "BGS rookie card",
    "SGC rookie card",
    "CGC rookie card",
    "PSA autograph card",
    "BGS autograph card",
    "SGC autograph card",
    "PSA patch card",
    "PSA prizm card",
    "PSA optic card",
    "downtown card PSA",
    "kaboom card PSA",
    "color blast PSA",
    "genesis PSA",
]

# ----------------------------------------


def sold_url(title: str) -> str:
    q = urllib.parse.quote(title)
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_Sold=1&LH_Complete=1"


def expected_profit(buy_price: float, buy_shipping: float) -> float:
    """
    Conservative profit estimate.
    We intentionally under estimate resale price.
    """
    resale_estimate = buy_price * 1.45

    resale_net = resale_estimate * (1 - SELL_FEE_RATE) - OUTBOUND_SHIP_SUPPLIES
    buy_in = buy_price * (1 + BUY_TAX_RATE) + buy_shipping

    return resale_net - buy_in


def run():
    print("SCANNER VERSION: PROFIT_25_ALL_SPORTS_LIVE")

    init_db()
    token = get_app_token()

    total_seen = 0
    total_profitable = 0
    total_inserted = 0

    for term in SEARCH_TERMS:
        print(f"SCANNER: query: {term}")

        data = browse_search(token, term)
        items = data.get("itemSummaries", [])

        print(f"SCANNER: items returned: {len(items)}")

        for item in items:
            total_seen += 1

            price = float(item["price"]["value"])
            shipping = 0.0

            profit = expected_profit(price, shipping)

            if profit < MIN_PROFIT:
                continue

            total_profitable += 1

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO deals
                        (item_id, title, item_url, sold_url, buy_price, buy_shipping, est_profit)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (item_id) DO NOTHING
                        """,
                        (
                            item["itemId"],
                            item["title"],
                            item["itemWebUrl"],
                            sold_url(item["title"]),
                            price,
                            shipping,
                            round(profit, 2),
                        ),
                    )
                    conn.commit()
                    total_inserted += 1

    print(f"SCANNER: total_seen: {total_seen}")
    print(f"SCANNER: total_profitable: {total_profitable}")
    print(f"SCANNER: total_inserted: {total_inserted}")


if __name__ == "__main__":
    run()
