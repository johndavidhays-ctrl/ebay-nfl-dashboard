import os
import time
import math
import random
import requests
from datetime import datetime, timezone

EBAY_BROWSE_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
EBAY_OAUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"

CLIENT_ID = os.environ.get("EBAY_CLIENT_ID")
CLIENT_SECRET = os.environ.get("EBAY_CLIENT_SECRET")

MIN_PROFIT = 150
MAX_RESULTS = 200
MAX_BIDS = 6
MAX_AUCTION_MINUTES = 60

HEADERS_BASE = {
    "Content-Type": "application/x-www-form-urlencoded",
}

SEARCH_QUERIES = [
    "rookie auto /10",
    "rookie auto /25",
    "rookie auto /49",
    "on card auto",
    "gold prizm /10",
    "black prizm",
    "contenders auto",
    "optic auto",
    "flawless patch auto",
    "immaculate auto",
]

BAD_KEYWORDS = [
    "lot",
    "lots",
    "binder",
    "bulk",
    "repack",
    "break",
]

def log(msg):
    print(f"{datetime.now().strftime('%H:%M:%S')} SCANNER: {msg}", flush=True)

def get_token():
    r = requests.post(
        EBAY_OAUTH_URL,
        headers=HEADERS_BASE,
        auth=(CLIENT_ID, CLIENT_SECRET),
        data={
            "grant_type": "client_credentials",
            "scope": "https://api.ebay.com/oauth/api_scope",
        },
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def request_with_backoff(headers, params):
    for attempt in range(6):
        r = requests.get(
            EBAY_BROWSE_SEARCH_URL,
            headers=headers,
            params=params,
            timeout=20,
        )

        if r.status_code == 200:
            return r.json()

        if r.status_code in (429, 500, 502, 503):
            sleep = min(30, (2 ** attempt) + random.uniform(0, 2))
            log(f"Rate limited. Sleeping {sleep:.1f}s")
            time.sleep(sleep)
            continue

        r.raise_for_status()

    raise RuntimeError("eBay request failed after retries")

def ends_within(item):
    if not item.get("itemEndDate"):
        return False
    end = datetime.fromisoformat(item["itemEndDate"].replace("Z", "+00:00"))
    minutes = (end - datetime.now(timezone.utc)).total_seconds() / 60
    return 0 < minutes <= MAX_AUCTION_MINUTES

def bad_title(title):
    t = title.lower()
    return any(bad in t for bad in BAD_KEYWORDS)

def estimate_market(token, title):
    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
    }

    params = {
        "q": title,
        "filter": "buyingOptions:{FIXED_PRICE}",
        "limit": 25,
    }

    data = request_with_backoff(headers, params)
    prices = []

    for i in data.get("itemSummaries", []):
        try:
            prices.append(float(i["price"]["value"]))
        except Exception:
            pass

    if len(prices) < 3:
        return None

    prices.sort()
    return sum(prices[len(prices)//3:]) / max(1, len(prices[len(prices)//3:]))

def scan():
    token = get_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
    }

    kept = 0
    seen = 0

    for q in SEARCH_QUERIES:
        log(f"query: {q}")

        params = {
            "q": q,
            "filter": "buyingOptions:{AUCTION}",
            "sort": "endingSoonest",
            "limit": MAX_RESULTS,
        }

        data = request_with_backoff(headers, params)

        for item in data.get("itemSummaries", []):
            seen += 1

            title = item.get("title", "")
            if bad_title(title):
                continue

            if not ends_within(item):
                continue

            bids = item.get("bidCount", 0)
            if bids > MAX_BIDS:
                continue

            try:
                current = float(item["price"]["value"])
            except Exception:
                continue

            market = estimate_market(token, title)
            if not market:
                continue

            profit = market - current
            if profit < MIN_PROFIT:
                continue

            kept += 1

            log(f"KEEP | ${current:.0f} -> ${market:.0f} | +${profit:.0f}")
            log(f"     {title}")

        time.sleep(3)

    log(f"seen: {seen}")
    log(f"kept: {kept}")

if __name__ == "__main__":
    scan()
