import urllib.parse
from app.db import init_db, get_conn
from app.ebay_auth import get_app_token
from app.ebay_browse import browse_search

MIN_BUY_PRICE = 10.0
MIN_PROFIT = 10.0
EBAY_FEE_RATE = 0.13

SEARCH_QUERIES = [
    "rookie card",
    "autograph card",
    "patch card",
    "jersey card",
    "numbered card /99",
    "numbered card /50",
    "numbered card /25",
    "silver prizm",
    "refractor",
    "holo",
    "ssp",
    "case hit",
    "prizm",
    "optic",
    "select",
    "mosaic",
    "bowman chrome",
    "topps chrome"
]

def sold_url(title):
    q = urllib.parse.quote(title)
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_Sold=1&LH_Complete=1"

def estimate_profit(price, shipping, estimated_sale):
    total_cost = price + shipping
    ebay_fee = estimated_sale * EBAY_FEE_RATE
    return estimated_sale - total_cost - ebay_fee

def calculate_score(profit, roi):
    return round((profit * 2) + (roi * 100), 2)

def run():
    init_db()
    token = get_app_token()

    total_seen = 0
    total_profitable = 0
    total_inserted = 0

    for query in SEARCH_QUERIES:
        print(f"SCANNER: query: {query}")
        data = browse_search(token, query)

        items = data.get("itemSummaries", [])
        print(f"SCANNER: items returned: {len(items)}")

        for item in items:
            total_seen += 1

            try:
                price = float(item["price"]["value"])
                shipping = float(
                    item.get("shippingOptions", [{}])[0]
                    .get("shippingCost", {})
                    .get("value", 0.0)
                )
            except Exception:
                continue

            if price < MIN_BUY_PRICE:
                continue

            estimated_sale = price * 1.4
            profit = estimate_profit(price, shipping, estimated_sale)

            if profit < MIN_PROFIT:
                continue

            roi = round(profit / (price + shipping), 2)
            score = calculate_score(profit, roi)

            listing_type = "AUCTION" if item.get("buyingOptions") == ["AUCTION"] else "BIN"

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
                            roi,
                            score,
                            listing_type
                        )
                    )
                    conn.commit()
                    total_inserted += 1
                    total_profitable += 1

    print(f"SCANNER: total_seen: {total_seen}")
    print(f"SCANNER: total_profitable: {total_profitable}")
    print(f"SCANNER: total_inserted: {total_inserted}")

if __name__ == "__main__":
    run()
