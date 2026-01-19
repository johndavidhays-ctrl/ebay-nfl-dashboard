import urllib.parse
from app.db import init_db, get_conn
from app.ebay_auth import get_app_token
from app.ebay_browse import browse_search

SCANNER_VERSION = "PROFIT_25_ALL_SPORTS_V5"

MIN_PROFIT = 25.0
SELL_FEE_RATE = 0.15
BUY_TAX_RATE = 0.06
OUTBOUND_SHIP_SUPPLIES = 6.50


def sold_url(title: str) -> str:
    q = urllib.parse.quote(title)
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_Sold=1&LH_Complete=1"


def estimate_profit(buy_price: float, shipping: float) -> float:
    resale_estimate = buy_price * 1.45
    resale_net = resale_estimate * (1 - SELL_FEE_RATE) - OUTBOUND_SHIP_SUPPLIES
    buy_total = buy_price * (1 + BUY_TAX_RATE) + shipping
    return resale_net - buy_total


def run():
    print(f"SCANNER VERSION: {SCANNER_VERSION}")

    init_db()
    token = get_app_token()
    print("SCANNER: token ok")

    queries = [
        "PSA graded card",
        "BGS graded card",
        "SGC graded card",
        "rookie card PSA",
        "autograph card PSA",
        "patch card PSA",
    ]

    total_seen = 0
    total_profitable = 0
    total_inserted = 0

    for q in queries:
        print(f"SCANNER: query: {q}")

        resp = browse_search(token, q)

        # browse_search returns a Response object
        if not
