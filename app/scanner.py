# app/scanner.py
import os
import re
import time
import math
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.db import init_db, mark_all_inactive, prune_inactive, upsert_deal


SCANNER_VERSION = "AUCTIONS_SINGLES_MINPROFIT150_V1"

EBAY_OAUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_BROWSE_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

MIN_PROFIT_USD = float(os.getenv("MIN_PROFIT_USD", "150"))
MAX_AUCTION_RESULTS = int(os.getenv("MAX_AUCTION_RESULTS", "50"))
MAX_FIXED_PRICE_RESULTS = int(os.getenv("MAX_FIXED_PRICE_RESULTS", "30"))

MAX_COMP_CALLS_PER_RUN = int(os.getenv("MAX_COMP_CALLS_PER_RUN", "10"))
SLEEP_BETWEEN_COMP_CALLS_SEC = float(os.getenv("SLEEP_BETWEEN_COMP_CALLS_SEC", "5"))
SLEEP_BETWEEN_AUCTION_QUERIES_SEC = float(os.getenv("SLEEP_BETWEEN_AUCTION_QUERIES_SEC", "2"))

MAX_ENDS_WITHIN_MINUTES = int(os.getenv("MAX_ENDS_WITHIN_MINUTES", str(24 * 60)))

# Keep it tight to reduce rate limiting and junk results
QUERIES = [
    "psa 10 football auto /10",
    "psa 10 football auto /25",
    "psa 10 football 1/1",
]

# Exclude obvious bulk and nonsense
BAD_TITLE_PATTERNS = [
    r"\blot\b",
    r"\blots\b",
    r"\bbinder\b",
    r"\bbulk\b",
    r"\bpack\b",
    r"\bblaster\b",
    r"\bbox\b",
    r"\bcase\b",
    r"\bbreak\b",
    r"\brepack\b",
    r"\bteam\b\s*lot\b",
    r"\bcollection\b",
    r"\bcomplete\s*set\b",
    r"\bbase\s*lot\b",
    r"\bcommons\b",
]

# Helpful for keeping only graded singles
GRADED_HINTS = [
    "PSA",
    "BGS",
    "SGC",
    "CGC",
    "HGA",
    "GEM",
    "MINT",
]


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"{ts} SCANNER: {msg}", flush=True)


def env_required(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing environment variable: {name}")
    return v


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1] + "+00:00"
        return datetime.fromisoformat(dt_str).astimezone(timezone.utc)
    except Exception:
        return None


def minutes_away(ends_at: Optional[datetime]) -> Optional[int]:
    if not ends_at:
        return None
    mins = (ends_at - now_utc()).total_seconds() / 60.0
    return int(math.floor(mins))


def money_value(obj: Optional[Dict[str, Any]]) -> float:
    if not obj:
        return 0.0
    try:
        return float(obj.get("value", 0.0))
    except Exception:
        return 0.0


def pick_currency(obj: Optional[Dict[str, Any]]) -> str:
    if not obj:
        return "USD"
    return obj.get("currency", "USD") or "USD"


def is_bad_title(title: str) -> bool:
    t = title.lower()
    for pat in BAD_TITLE_PATTERNS:
        if re.search(pat, t, flags=re.IGNORECASE):
            return True
    return False


def looks_graded(title: str) -> bool:
    upper = title.upper()
    return any(h in upper for h in GRADED_HINTS)


def cleanup_title_for_comps(title: str) -> str:
    t = title.upper()

    # Remove obvious filler
    t = re.sub(r"\bHOT\b", " ", t)
    t = re.sub(r"\bRARE\b", " ", t)
    t = re.sub(r"\bSSP\b", " ", t)
    t = re.sub(r"\bSP\b", " ", t)
    t = re.sub(r"\bLOOK\b", " ", t)

    # Normalize grading text
    t = re.sub(r"\bGEM\s*MINT\s*10\b", " PSA 10 ", t)
    t = re.sub(r"\bPSA\s*GEM\s*MT\s*10\b", " PSA 10 ", t)
    t = re.sub(r"\bPSA\s*10\b", " PSA 10 ", t)
    t = re.sub(r"\bBGS\s*10\b", " BGS 10 ", t)
    t = re.sub(r"\bSGC\s*10\b", " SGC 10 ", t)

    # Remove serial number slash chunk like 35/175 or /99 etc
    t = re.sub(r"\b\d{1,4}\s*/\s*\d{1,4}\b", " ", t)
    t = re.sub(r"\b/\s*\d{1,4}\b", " ", t)

    # Remove obvious year noise but keep first year if present
    # We do not strip all years because it helps matching sets
    # Keep it simple: no change here

    # Collapse spaces
    t = re.sub(r"[^A-Z0-9 ]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    # Make it shorter to reduce query length
    words = t.split()
    if len(words) > 14:
        words = words[:14]
    return " ".join(words)


def ebay_token() -> str:
    client_id = env_required("EBAY_CLIENT_ID")
    client_secret = env_required("EBAY_CLIENT_SECRET")

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope",
    }

    r = requests.post(
        EBAY_OAUTH_URL,
        headers=headers,
        data=data,
        auth=(client_id, client_secret),
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"eBay token error {r.status_code}: {r.text[:500]}")
    j = r.json()
    token = j.get("access_token", "")
    if not token:
        raise RuntimeError("eBay token missing access_token")
    return token


def request_with_backoff(method: str, url: str, headers: Dict[str, str], params: Dict[str, Any]) -> Dict[str, Any]:
    # Gentle backoff for 429, plus retry on some 5xx
    backoff = 2.0
    for attempt in range(1, 8):
        r = requests.request(method, url, headers=headers, params=params, timeout=45)

        if r.status_code == 429:
            sleep_for = backoff + random.random()
            log(f"Rate limited by eBay. Sleeping {sleep_for:.1f} seconds.")
            time.sleep(sleep_for)
            backoff = min(backoff * 1.8, 30.0)
            continue

        if r.status_code in (500, 502, 503, 504):
            sleep_for = backoff + random.random()
            log(f"eBay server error {r.status_code}. Sleeping {sleep_for:.1f} seconds.")
            time.sleep(sleep_for)
            backoff = min(backoff * 1.8, 30.0)
            continue

        if r.status_code == 401:
            raise RuntimeError(f"401 Unauthorized from eBay. Check EBAY_CLIENT_ID and EBAY_CLIENT_SECRET. {r.text[:300]}")

        if r.status_code >= 400:
            raise RuntimeError(f"eBay error {r.status_code}: {r.text[:500]}")

        try:
            return r.json()
        except Exception:
            raise RuntimeError(f"eBay invalid JSON: {r.text[:500]}")

    raise RuntimeError("eBay request failed after retries")


def ebay_search(token: str, query: str, buying_option: str, limit: int) -> List[Dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # buy browse filters
    # buyingOption values used by eBay Browse: AUCTION, FIXED_PRICE
    params = {
        "q": query,
        "limit": str(limit),
        "sort": "endingSoonest",
        "filter": f"buyingOptions:{{{buying_option}}}",
    }

    j = request_with_backoff("GET", EBAY_BROWSE_SEARCH_URL, headers=headers, params=params)
    return j.get("itemSummaries", []) or []


def total_cost_from_item(item: Dict[str, Any]) -> Tuple[float, str]:
    # Use currentPrice or price, plus first shipping cost if present
    price_obj = item.get("currentBidPrice") or item.get("price") or item.get("currentPrice") or {}
    currency = pick_currency(price_obj)
    price_val = money_value(price_obj)

    ship_val = 0.0
    ship = item.get("shippingOptions") or []
    if ship and isinstance(ship, list):
        s0 = ship[0] or {}
        ship_cost = s0.get("shippingCost") or {}
        if pick_currency(ship_cost) == currency:
            ship_val = money_value(ship_cost)

    return price_val + ship_val, currency


def ends_at_from_item(item: Dict[str, Any]) -> Optional[datetime]:
    # eBay browse uses itemEndDate for auctions
    return parse_iso(item.get("itemEndDate"))


def robust_market_from_fixed_prices(items: List[Dict[str, Any]]) -> Tuple[float, int]:
    prices: List[float] = []
    for it in items:
        price_obj = it.get("price") or {}
        val = money_value(price_obj)
        if val <= 0:
            continue
        prices.append(val)

    if len(prices) < 5:
        return 0.0, len(prices)

    prices.sort()

    # Trim top and bottom 20 percent to cut junk
    n = len(prices)
    lo = int(math.floor(n * 0.2))
    hi = int(math.ceil(n * 0.8))
    core = prices[lo:hi] if hi > lo else prices

    if not core:
        return 0.0, len(prices)

    core.sort()
    mid = len(core) // 2
    if len(core) % 2 == 1:
        median = core[mid]
    else:
        median = (core[mid - 1] + core[mid]) / 2.0

    return float(median), len(prices)


def estimate_market_from_fixed_price(token: str, auction_title: str) -> Tuple[float, int]:
    comp_q = cleanup_title_for_comps(auction_title)
    if not comp_q:
        return 0.0, 0

    fixed_items = ebay_search(token, comp_q, "FIXED_PRICE", limit=MAX_FIXED_PRICE_RESULTS)
    market, comp_count = robust_market_from_fixed_prices(fixed_items)
    return market, comp_count


def item_url(item: Dict[str, Any]) -> Optional[str]:
    # eBay browse: itemWebUrl
    return item.get("itemWebUrl")


def image_url(item: Dict[str, Any]) -> Optional[str]:
    img = item.get("image") or {}
    return img.get("imageUrl")


def should_keep_auction(title: str, ends: Optional[datetime]) -> bool:
    if not title:
        return False
    if is_bad_title(title):
        return False

    # Keep only graded singles for now
    if not looks_graded(title):
        return False

    if not ends:
        return False
    mins = minutes_away(ends)
    if mins is None:
        return False
    if mins < 0:
        return False
    if mins > MAX_ENDS_WITHIN_MINUTES:
        return False

    return True


def run() -> None:
    log(f"SCANNER VERSION: {SCANNER_VERSION}")

    init_db()
    token = ebay_token()

    mark_all_inactive()

    seen = 0
    kept = 0
    inserted = 0
    comp_calls = 0

    for q in QUERIES:
        log(f"query: {q}")
        time.sleep(SLEEP_BETWEEN_AUCTION_QUERIES_SEC)

        auctions = ebay_search(token, q, "AUCTION", limit=MAX_AUCTION_RESULTS)
        log(f"items returned: {len(auctions)}")

        for it in auctions:
            seen += 1

            title = (it.get("title") or "").strip()
            ends = ends_at_from_item(it)

            if not should_keep_auction(title, ends):
                continue

            total_cost, currency = total_cost_from_item(it)
            if currency != "USD":
                # Skip non USD to avoid weird profit math
                continue

            market = 0.0
            comp_count = 0

            if comp_calls < MAX_COMP_CALLS_PER_RUN:
                time.sleep(SLEEP_BETWEEN_COMP_CALLS_SEC)
                market, comp_count = estimate_market_from_fixed_price(token, title)
                comp_calls += 1
            else:
                # No more comps this run, skip because we cannot price it
                continue

            if market <= 0:
                continue

            profit = market - total_cost

            if profit < MIN_PROFIT_USD:
                continue

            kept += 1

            deal = {
                "item_id": it.get("itemId"),
                "title": title,
                "url": item_url(it),
                "image_url": image_url(it),
                "query": q,
                "total_cost": float(round(total_cost, 2)),
                "market": float(round(market, 2)),
                "profit": float(round(profit, 2)),
                "ends_at": ends.isoformat() if ends else None,
                "minutes_away": minutes_away(ends),
                "is_active": True,
            }

            # Upsert into DB
            upsert_deal(deal)
            inserted += 1

        log(f"kept so far: {kept}")

    pruned = prune_inactive()
    log(f"seen: {seen}")
    log(f"kept: {kept}")
    log(f"inserted: {inserted}")
    log(f"pruned_inactive: {pruned}")


if __name__ == "__main__":
    run()
