import os
import re
import time
import math
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.db import init_db, mark_all_inactive, prune_inactive, upsert_deal


SCANNER_VERSION = "CHAOS_ALL_DIRECTIONS_V1"

EBAY_OAUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

MARKETPLACE_ID = os.getenv("EBAY_MARKETPLACE_ID", "EBAY_US").strip() or "EBAY_US"

MIN_PROFIT_USD = float(os.getenv("MIN_PROFIT_USD", "150"))
MAX_AUCTION_RESULTS = int(os.getenv("MAX_AUCTION_RESULTS", "60"))
MAX_FIXED_PRICE_RESULTS = int(os.getenv("MAX_FIXED_PRICE_RESULTS", "35"))

# Guardrails for speed and rate limits
MAX_COMP_CALLS_PER_RUN = int(os.getenv("MAX_COMP_CALLS_PER_RUN", "18"))
SLEEP_BETWEEN_COMP_CALLS_SEC = float(os.getenv("SLEEP_BETWEEN_COMP_CALLS_SEC", "4"))
SLEEP_BETWEEN_QUERIES_SEC = float(os.getenv("SLEEP_BETWEEN_QUERIES_SEC", "2"))

# Keep only auctions ending soon, this is where quick flips happen
MAX_ENDS_WITHIN_MINUTES = int(os.getenv("MAX_ENDS_WITHIN_MINUTES", "90"))

# Keep only listings with low attention
MAX_BID_COUNT = int(os.getenv("MAX_BID_COUNT", "6"))

# Profit model, conservative
FEE_RATE = float(os.getenv("FEE_RATE", "0.1325"))
FIXED_FEE = float(os.getenv("FIXED_FEE", "0.30"))
MARKET_HAIRCUT = float(os.getenv("MARKET_HAIRCUT", "0.92"))

# If you want to cap buy price to avoid tying up cash
MAX_TOTAL_COST = float(os.getenv("MAX_TOTAL_COST", "800"))

# Optional sports trading cards category id
# Leave empty for broader scanning including miscategorized listings
CATEGORY_IDS = os.getenv("EBAY_CATEGORY_IDS", "").strip()


BAD_TITLE_WORDS = [
    "lot",
    "lots",
    "binder",
    "collection",
    "bulk",
    "repack",
    "break",
    "breaks",
    "mystery",
    "you pick",
    "you choose",
    "random",
    "pack",
    "packs",
    "blaster",
    "box",
    "case",
    "team lot",
    "player lot",
    "base lot",
    "commons",
]

GOOD_SIGNAL_WORDS = [
    "auto",
    "autograph",
    "on card",
    "rookie auto",
    "rc auto",
    "patch",
    "rpa",
    "logo",
    "shield",
    "laundry",
    "tag",
    "booklet",
    "one of one",
    "1/1",
    "ssp",
    "case hit",
    "gold",
    "black",
    "red",
    "blue",
    "green",
    "pink",
    "orange",
    "silver",
    "refractor",
    "prizm",
    "optic",
    "select",
    "contenders",
    "flawless",
    "immaculate",
    "national treasures",
    "nt",
    "downtown",
    "kaboom",
    "color blast",
]

# Misspelling bait. These show up underbid all the time.
MISSPELL_QUERIES = [
    "jamarr chase auto",
    "jamar chase auto",
    "cj stroud rookie",
    "stroud cj rookie",
    "pat mahomes prizm",
    "pattrick mahomes prizm",
    "justin jeffersons auto",
    "jefferson justin auto",
    "brock purdy rooky",
    "anthony richardson rooky auto",
]

# Core money searches. Raw and graded both allowed.
CORE_QUERIES = [
    "rookie auto /10",
    "rookie auto /25",
    "rookie auto /49",
    "rookie auto /99",
    "on card auto /25",
    "gold prizm /10",
    "black prizm 1/1",
    "contenders auto",
    "optic auto",
    "flawless patch auto",
    "immaculate auto",
    "national treasures rpa",
    "downtown",
    "kaboom",
    "color blast",
]

# Odd category angle. These are narrower so we can search without category filter.
MISCAT_QUERIES = [
    "football auto /10",
    "basketball auto /10",
    "rookie autograph /25",
    "prizm /10",
    "1/1 auto",
]

STOP_WORDS = {
    "hot", "rare", "nice", "great", "awesome", "mint", "gem", "mt",
    "card", "cards", "rc", "rookie", "auto", "autograph", "on", "carded",
    "the", "a", "an", "of", "and", "with", "for", "to", "by",
}


def log(msg: str) -> None:
    print(f"SCANNER: {msg}", flush=True)


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


def minutes_away(dt: Optional[datetime]) -> Optional[int]:
    if not dt:
        return None
    mins = int((dt - now_utc()).total_seconds() // 60)
    return mins


def money_value(obj: Optional[Dict[str, Any]]) -> float:
    if not obj:
        return 0.0
    try:
        return float(obj.get("value", 0.0))
    except Exception:
        return 0.0


def item_bid_count(item: Dict[str, Any]) -> int:
    try:
        return int(item.get("bidCount") or 0)
    except Exception:
        return 0


def is_bad_title(title: str) -> bool:
    t = (title or "").lower()
    if re.search(r"\b\d+\s*cards?\b", t):
        return True
    for w in BAD_TITLE_WORDS:
        if w in t:
            return True
    return False


def has_good_signals(title: str) -> bool:
    t = (title or "").lower()
    if "/1" in t or "1/1" in t:
        return True
    if re.search(r"\b/\s*\d{1,4}\b", t):
        return True
    if re.search(r"\b\d{1,4}\s*/\s*\d{1,4}\b", t):
        return True
    for w in GOOD_SIGNAL_WORDS:
        if w in t:
            return True
    return False


def get_oauth_token() -> str:
    client_id = env_required("EBAY_CLIENT_ID")
    client_secret = env_required("EBAY_CLIENT_SECRET")

    r = requests.post(
        EBAY_OAUTH_URL,
        auth=(client_id, client_secret),
        data={
            "grant_type": "client_credentials",
            "scope": "https://api.ebay.com/oauth/api_scope",
        },
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"eBay OAuth error {r.status_code}: {r.text[:300]}")
    j = r.json()
    token = j.get("access_token", "")
    if not token:
        raise RuntimeError("Missing access_token from eBay OAuth response")
    return token


def request_with_backoff(
    token: str,
    params: Dict[str, Any],
    max_attempts: int = 6,
) -> Optional[Dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": MARKETPLACE_ID,
        "Accept": "application/json",
    }

    backoff = 2.0
    for attempt in range(1, max_attempts + 1):
        r = requests.get(EBAY_SEARCH_URL, headers=headers, params=params, timeout=35)

        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                return None

        if r.status_code == 401:
            return None

        if r.status_code == 429 or r.status_code in (500, 502, 503, 504):
            sleep_for = min(30.0, backoff + random.random())
            log(f"rate limited or server error {r.status_code}, sleep {sleep_for:.1f}s")
            time.sleep(sleep_for)
            backoff = min(backoff * 1.8, 30.0)
            continue

        if r.status_code >= 400:
            log(f"ebay error {r.status_code}: {r.text[:200]}")
            return None

    return None


def ebay_search(
    token: str,
    query: str,
    buying_option: str,
    limit: int,
    sort: str = "endingSoonest",
    use_category: bool = True,
) -> List[Dict[str, Any]]:
    filters = [f"buyingOptions:{{{buying_option}}}"]

    params: Dict[str, Any] = {
        "q": query,
        "limit": str(limit),
        "sort": sort,
        "filter": ",".join(filters),
    }

    if use_category and CATEGORY_IDS:
        params["category_ids"] = CATEGORY_IDS

    j = request_with_backoff(token, params)
    if not j:
        return []
    return j.get("itemSummaries", []) or []


def total_cost_from_item(item: Dict[str, Any]) -> float:
    price_obj = item.get("currentBidPrice") or item.get("price") or item.get("currentPrice") or {}
    price_val = money_value(price_obj)

    ship_val = 0.0
    ship_opts = item.get("shippingOptions") or []
    if ship_opts:
        s0 = ship_opts[0] or {}
        ship_cost = s0.get("shippingCost") or {}
        ship_val = money_value(ship_cost)

    return float(price_val + ship_val)


def ends_at_from_item(item: Dict[str, Any]) -> Optional[datetime]:
    return parse_iso(item.get("itemEndDate"))


def build_comp_query(title: str) -> str:
    t = (title or "")
    t = re.sub(r"\([^)]*\)", " ", t)
    t = re.sub(r"\[[^\]]*\]", " ", t)
    t = re.sub(r"[^A-Za-z0-9\s/#]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    tokens: List[str] = []
    for raw in t.split(" "):
        low = raw.lower()
        if low in STOP_WORDS:
            continue
        if len(low) <= 1:
            continue
        tokens.append(raw)

    if not tokens:
        return t[:120]
    return " ".join(tokens[:12])[:120]


def median(values: List[float]) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    n = len(values)
    mid = n // 2
    if n % 2 == 1:
        return float(values[mid])
    return float((values[mid - 1] + values[mid]) / 2.0)


class CompCache:
    def __init__(self) -> None:
        self.store: Dict[str, Tuple[float, int]] = {}

    def get(self, key: str) -> Optional[Tuple[float, int]]:
        return self.store.get(key)

    def set(self, key: str, value: Tuple[float, int]) -> None:
        self.store[key] = value


def estimate_market_fixed_price(
    token: str,
    title: str,
    cache: CompCache,
) -> Tuple[float, int]:
    comp_q = build_comp_query(title)
    cached = cache.get(comp_q)
    if cached:
        return cached[0], cached[1]

    items = ebay_search(
        token,
        comp_q,
        buying_option="FIXED_PRICE",
        limit=MAX_FIXED_PRICE_RESULTS,
        sort="bestMatch",
        use_category=True,
    )

    prices: List[float] = []
    for it in items:
        p = money_value(it.get("price") or {})
        if p > 0:
            prices.append(float(p))

    if len(prices) < 6:
        cache.set(comp_q, (0.0, len(prices)))
        return 0.0, len(prices)

    prices.sort()
    trim = int(len(prices) * 0.2)
    core = prices[trim: len(prices) - trim] if len(prices) - 2 * trim >= 5 else prices

    m = median(core)
    cache.set(comp_q, (m, len(prices)))
    return m, len(prices)


def profit_estimate(market: float, total_cost: float) -> float:
    net_sale = (market * MARKET_HAIRCUT)
    fees = (net_sale * FEE_RATE) + FIXED_FEE
    return net_sale - fees - total_cost


def should_consider_item(title: str, ends_at: Optional[datetime], bids: int, total_cost: float) -> bool:
    if not title:
        return False
    if is_bad_title(title):
        return False

    if not ends_at:
        return False
    mins = minutes_away(ends_at)
    if mins is None or mins < 0 or mins > MAX_ENDS_WITHIN_MINUTES:
        return False

    if bids > MAX_BID_COUNT:
        return False

    if total_cost <= 0 or total_cost > MAX_TOTAL_COST:
        return False

    # Fast upside heuristic, skip boring base cards unless mislisted queries
    if not has_good_signals(title):
        return False

    return True


def run() -> None:
    log(f"SCANNER VERSION: {SCANNER_VERSION}")

    init_db()

    token = get_oauth_token()

    mark_all_inactive()

    cache = CompCache()

    seen = 0
    kept = 0
    comp_calls = 0

    # Strategy bundle
    # 1) Core upside
    # 2) Misspell bait
    # 3) Miscat narrow searches without category filter
    queries: List[Tuple[str, bool]] = []
    for q in CORE_QUERIES:
        queries.append((q, True))
    for q in MISSPELL_QUERIES:
        queries.append((q, True))
    for q in MISCAT_QUERIES:
        queries.append((q, False))

    for q, use_category in queries:
        time.sleep(SLEEP_BETWEEN_QUERIES_SEC)
        log(f"query: {q}")

        auctions = ebay_search(
            token,
            q,
            buying_option="AUCTION",
            limit=MAX_AUCTION_RESULTS,
            sort="endingSoonest",
            use_category=use_category,
        )

        log(f"items returned: {len(auctions)}")

        for it in auctions:
            seen += 1

            title = (it.get("title") or "").strip()
            item_id = it.get("itemId")
            if not item_id or not title:
                continue

            ends_at = ends_at_from_item(it)
            bids = item_bid_count(it)
            total_cost = total_cost_from_item(it)

            if not should_consider_item(title, ends_at, bids, total_cost):
                continue

            if comp_calls >= MAX_COMP_CALLS_PER_RUN:
                continue

            time.sleep(SLEEP_BETWEEN_COMP_CALLS_SEC)

            market, comp_count = estimate_market_fixed_price(token, title, cache)
            comp_calls += 1

            if market <= 0:
                continue

            profit = profit_estimate(market, total_cost)
            if profit < MIN_PROFIT_USD:
                continue

            deal = {
                "item_id": str(item_id),
                "title": title,
                "url": it.get("itemWebUrl"),
                "image_url": (it.get("image") or {}).get("imageUrl"),
                "query": q,
                "total_cost": float(round(total_cost, 2)),
                "market": float(round(market, 2)),
                "profit": float(round(profit, 2)),
                "ends_at": ends_at,
                "is_active": True,
            }

            upsert_deal(deal)
            kept += 1

        log(f"kept so far: {kept}")

    pruned = prune_inactive()
    log(f"seen: {seen}")
    log(f"kept: {kept}")
    log(f"comp_calls: {comp_calls}")
    log(f"pruned_inactive: {pruned}")


if __name__ == "__main__":
    run()
