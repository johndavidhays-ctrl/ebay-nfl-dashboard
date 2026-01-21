# app/scanner.py
import os
import re
import sys
import time
import math
import random
import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from sqlalchemy import create_engine, text


SCANNER_VERSION = "HYBRID_UPSIDE_MINPROFIT150_V1"

EBAY_OAUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_BROWSE_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

MIN_PROFIT = float(os.getenv("MIN_PROFIT", "150"))
MAX_AUCTION_RESULTS_PER_QUERY = int(os.getenv("MAX_AUCTION_RESULTS_PER_QUERY", "80"))
MAX_FIXED_RESULTS_PER_COMP = int(os.getenv("MAX_FIXED_RESULTS_PER_COMP", "30"))

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))

# Safety pacing to reduce 429
MIN_SLEEP_BETWEEN_CALLS_SEC = float(os.getenv("MIN_SLEEP_BETWEEN_CALLS_SEC", "0.6"))
MAX_CALLS_PER_RUN = int(os.getenv("MAX_CALLS_PER_RUN", "350"))

# Backoff behavior
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "7"))
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "1.8"))
BACKOFF_JITTER = float(os.getenv("BACKOFF_JITTER", "0.35"))

# Filter knobs
REQUIRE_AUCTION_ONLY = True
EXCLUDE_LOTS = True

# Optional: narrow to cards only if you want
# If you want to be stricter, set EBAY_CATEGORY_ID to "212" (Sports Trading Cards)
EBAY_CATEGORY_ID = os.getenv("EBAY_CATEGORY_ID", "").strip()

# Extra fees buffer (platform fees, shipping uncertainty)
FEE_BUFFER_RATE = float(os.getenv("FEE_BUFFER_RATE", "0.13"))  # 13 percent default
FEE_BUFFER_FLAT = float(os.getenv("FEE_BUFFER_FLAT", "3.00"))  # 3 dollars default


def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%H:%M:%S")
    print(f"{ts} SCANNER: {msg}", flush=True)


@dataclass
class Deal:
    item_id: str
    title: str
    url: Optional[str]
    image_url: Optional[str]
    query: str
    total_cost: float
    market: float
    profit: float
    ends_at: Optional[dt.datetime]
    minutes_away: Optional[int]


class Budget:
    def __init__(self, max_calls: int):
        self.max_calls = max_calls
        self.calls = 0
        self.last_call_at = 0.0

    def can_call(self) -> bool:
        return self.calls < self.max_calls

    def mark_call(self) -> None:
        self.calls += 1
        self.last_call_at = time.time()

    def pace(self) -> None:
        now = time.time()
        elapsed = now - self.last_call_at
        if elapsed < MIN_SLEEP_BETWEEN_CALLS_SEC:
            time.sleep(MIN_SLEEP_BETWEEN_CALLS_SEC - elapsed)


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_iso_dt(s: Optional[str]) -> Optional[dt.datetime]:
    if not s:
        return None
    try:
        # eBay often returns ISO 8601 with Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def minutes_until(end: Optional[dt.datetime]) -> Optional[int]:
    if not end:
        return None
    diff = (end - now_utc()).total_seconds()
    return max(0, int(diff // 60))


def money(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def get_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def get_ebay_token() -> str:
    client_id = get_env("EBAY_CLIENT_ID")
    client_secret = get_env("EBAY_CLIENT_SECRET")

    auth = (client_id, client_secret)
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope",
    }

    r = requests.post(
        EBAY_OAUTH_URL,
        headers=headers,
        data=data,
        auth=auth,
        timeout=REQUEST_TIMEOUT,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Token request failed: {r.status_code} {r.text[:300]}")
    j = r.json()
    return j["access_token"]


def is_rate_limited(resp: requests.Response) -> bool:
    if resp.status_code == 429:
        return True
    # Sometimes eBay returns 500 range while throttling
    if resp.status_code in (500, 502, 503, 504):
        return True
    return False


def request_with_backoff(
    budget: Budget,
    method: str,
    url: str,
    headers: Dict[str, str],
    params: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    """
    Returns (json, status_code).
    If rate limited after retries, returns ({}, last_status) so the run ends safely.
    """
    last_status = None

    for attempt in range(1, MAX_RETRIES + 1):
        if not budget.can_call():
            log("Call budget reached, ending scan safely.")
            return {}, 200

        budget.pace()
        budget.mark_call()

        try:
            r = requests.request(
                method,
                url,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            last_status = r.status_code

            if r.status_code == 200:
                return r.json(), r.status_code

            if is_rate_limited(r):
                sleep_s = (BACKOFF_BASE ** attempt) + random.random() * BACKOFF_JITTER
                sleep_s = min(sleep_s, 35.0)
                log(f"rate limited {r.status_code}, sleeping {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue

            # Non retryable
            log(f"request failed {r.status_code}: {r.text[:250]}")
            return {}, r.status_code

        except requests.RequestException as e:
            sleep_s = (BACKOFF_BASE ** attempt) + random.random() * BACKOFF_JITTER
            sleep_s = min(sleep_s, 35.0)
            log(f"network error, sleeping {sleep_s:.1f}s: {e}")
            time.sleep(sleep_s)
            continue

    log("eBay still rate limited after retries. Ending scan safely.")
    return {}, last_status


def normalize_title_for_comp(title: str) -> str:
    t = title.lower()

    # Remove common noise
    t = re.sub(r"\bpsa\s*\d+\b", " ", t)
    t = re.sub(r"\bbgs\s*\d+(\.\d+)?\b", " ", t)
    t = re.sub(r"\bsgc\s*\d+\b", " ", t)
    t = re.sub(r"\bpop\s*\d+\b", " ", t)
    t = re.sub(r"\b(patch|jersey|lot)\b", " ", t)

    # Remove serial formats to broaden comps slightly
    t = re.sub(r"\b\d+\s*/\s*\d+\b", " ", t)
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    # Keep first N words to avoid overly long query
    words = t.split()
    return " ".join(words[:10])


def extract_price(item: Dict[str, Any]) -> float:
    price = item.get("price") or {}
    val = price.get("value")
    return money(val)


def extract_total_cost(item: Dict[str, Any]) -> float:
    """
    Use price + shipping if present.
    """
    p = extract_price(item)
    ship = 0.0
    shipping = item.get("shippingOptions") or item.get("shippingOption") or item.get("shipping") or {}
    # Browse API varies. Try best guess.
    if isinstance(shipping, dict):
        ship_cost = shipping.get("shippingCost") or {}
        ship = money(ship_cost.get("value"))
    elif isinstance(shipping, list) and shipping:
        ship_cost = shipping[0].get("shippingCost") or {}
        ship = money(ship_cost.get("value"))
    return max(0.0, p + ship)


def looks_like_lot(title: str) -> bool:
    t = title.lower()
    lot_words = [
        "lot", "lots", "bundle", "mystery", "pack", "packs", "box", "boxes",
        "break", "team", "random", "binder", "collection", "bulk",
        "set", "complete set", "case",
        "multiple", "assorted", "mixed",
        "cards", "100+", "200+", "300+",
    ]
    for w in lot_words:
        if w in t:
            return True
    # "x cards" pattern
    if re.search(r"\b\d+\s*(card|cards)\b", t):
        return True
    return False


def looks_like_card(title: str) -> bool:
    t = title.lower()
    # Keep it broad: sports, tcg, entertainment can still flip
    return any(k in t for k in ["card", "rookie", "auto", "autograph", "rc", "prizm", "optic", "topps", "panini"])


def ebay_search(
    budget: Budget,
    token: str,
    q: str,
    buying_option: str,
    limit: int,
    sort: str,
) -> List[Dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}"}

    params: Dict[str, Any] = {
        "q": q,
        "limit": min(max(1, limit), 200),
        "sort": sort,
        "filter": f"buyingOptions:{{{buying_option}}}",
    }

    if EBAY_CATEGORY_ID:
        params["category_ids"] = EBAY_CATEGORY_ID

    # Prefer US results for consistency
    params["fieldgroups"] = "MATCHING_ITEMS"

    j, _status = request_with_backoff(
        budget=budget,
        method="GET",
        url=EBAY_BROWSE_SEARCH_URL,
        headers=headers,
        params=params,
    )

    if not j or not isinstance(j, dict):
        return []

    items = j.get("itemSummaries") or []
    if not isinstance(items, list):
        return []
    return items


def estimate_market_from_fixed_price(
    budget: Budget,
    token: str,
    title: str,
) -> Tuple[float, int]:
    """
    Approximates market value from similar fixed price listings.
    This is not perfect. It is the best you can do with Browse API alone.
    """
    comp_q = normalize_title_for_comp(title)
    if not comp_q:
        return 0.0, 0

    items = ebay_search(
        budget=budget,
        token=token,
        q=comp_q,
        buying_option="FIXED_PRICE",
        limit=MAX_FIXED_RESULTS_PER_COMP,
        sort="bestMatch",
    )
    prices = []
    for it in items:
        p = extract_price(it)
        if p > 0:
            prices.append(p)

    if not prices:
        return 0.0, 0

    prices.sort()
    # Use median for stability
    mid = prices[len(prices) // 2]
    return float(mid), len(prices)


def fee_adjusted_profit(market: float, total_cost: float) -> float:
    """
    Conservative profit after a fee buffer and a small flat buffer.
    """
    if market <= 0:
        return -999999.0
    fees = (market * FEE_BUFFER_RATE) + FEE_BUFFER_FLAT
    return market - total_cost - fees


def build_queries() -> List[str]:
    """
    Wide net for quick upside.
    Focus on scarcity, premium inserts, true short prints, and on card autos.
    """
    base = [
        # Serial and true scarcity
        "1/1 card",
        "logoman card",
        "shield patch auto",
        "gold vinyl /5",
        "black finite 1/1",
        "black finite card",
        "gold /10 card",
        "green shimmer /5",
        "nebula 1/1",
        "mojo /25",
        "blue shimmer /25",
        "red shimmer /9",
        "gold shimmer /10",
        "tie dye /25",

        # Case hits and premium inserts
        "downtown card",
        "kaboom card",
        "color blast card",
        "stained glass card",
        "night moves card",
        "anime card",
        "color wheel card",
        "radiation card",
        "black colorblast",

        # Autos and rookie premium
        "on card auto /25",
        "rookie auto /10",
        "rpa /25",
        "rookie patch auto /49",
        "contenders auto /25",
        "national treasures rpa",
        "impeccable on card auto",
        "flawless auto /25",

        # Graded but not limited to PSA 10
        "psa 9 auto /25",
        "bgs 9.5 auto /25",
        "sgc 10 auto /25",
        "gem mint /10",

        # Weird but high upside
        "missing card number psa",
        "error card psa",
        "variation ssp",
        "short print ssp",
        "superfractor 1/1",
        "printing plate 1/1",
    ]

    # Keep some broader lanes for new product hype
    base += [
        "prizm color match /25",
        "select zebra ssp",
        "optic holo gold /10",
        "mosaic genesis ssp",
        "mosaic peacock ssp",
    ]

    # Deduplicate while preserving order
    seen = set()
    out = []
    for q in base:
        if q not in seen:
            out.append(q)
            seen.add(q)
    return out


def create_engine_from_env():
    db_url = get_env("DATABASE_URL")
    return create_engine(db_url, pool_pre_ping=True)


def init_db(engine) -> None:
    """
    Creates a deals table with item_id as primary key.
    No id column, which avoids the error you were getting.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS deals (
        item_id TEXT PRIMARY KEY,
        title TEXT,
        url TEXT,
        image_url TEXT,
        query TEXT,
        total_cost DOUBLE PRECISION,
        market DOUBLE PRECISION,
        profit DOUBLE PRECISION,
        ends_at TIMESTAMPTZ,
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def mark_all_inactive(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("UPDATE deals SET is_active = FALSE, updated_at = NOW();"))


def prune_inactive(engine, older_than_hours: int = 72) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM deals
                WHERE is_active = FALSE
                  AND updated_at < NOW() - (:hrs || ' hours')::interval;
                """
            ),
            {"hrs": int(older_than_hours)},
        )


def upsert_deal(engine, d: Deal) -> None:
    sql = """
    INSERT INTO deals (
        item_id, title, url, image_url, query,
        total_cost, market, profit, ends_at,
        is_active, created_at, updated_at
    )
    VALUES (
        :item_id, :title, :url, :image_url, :query,
        :total_cost, :market, :profit, :ends_at,
        TRUE, NOW(), NOW()
    )
    ON CONFLICT (item_id) DO UPDATE SET
        title = EXCLUDED.title,
        url = EXCLUDED.url,
        image_url = EXCLUDED.image_url,
        query = EXCLUDED.query,
        total_cost = EXCLUDED.total_cost,
        market = EXCLUDED.market,
        profit = EXCLUDED.profit,
        ends_at = EXCLUDED.ends_at,
        is_active = TRUE,
        updated_at = NOW();
    """
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "item_id": d.item_id,
                "title": d.title,
                "url": d.url,
                "image_url": d.image_url,
                "query": d.query,
                "total_cost": float(d.total_cost),
                "market": float(d.market),
                "profit": float(d.profit),
                "ends_at": d.ends_at,
            },
        )


def pick_url(item: Dict[str, Any]) -> Optional[str]:
    return item.get("itemWebUrl") or item.get("itemHref") or item.get("webUrl")


def pick_image(item: Dict[str, Any]) -> Optional[str]:
    img = item.get("image") or {}
    if isinstance(img, dict):
        return img.get("imageUrl")
    return None


def pick_ends_at(item: Dict[str, Any]) -> Optional[dt.datetime]:
    # Browse API uses different shapes
    info = item.get("listingInfo") or {}
    if isinstance(info, dict):
        end = info.get("endTime")
        return parse_iso_dt(end)
    return None


def scan() -> None:
    log(f"SCANNER VERSION: {SCANNER_VERSION}")
    token = get_ebay_token()
    engine = create_engine_from_env()
    init_db(engine)

    budget = Budget(MAX_CALLS_PER_RUN)

    queries = build_queries()
    log(f"queries: {len(queries)}")
    log(f"min_profit: {MIN_PROFIT:.2f}")
    log(f"auction_limit_per_query: {MAX_AUCTION_RESULTS_PER_QUERY}")

    mark_all_inactive(engine)

    seen = 0
    kept = 0

    for q in queries:
        if not budget.can_call():
            log("budget reached, ending scan safely.")
            break

        log(f"query: {q}")

        auctions = ebay_search(
            budget=budget,
            token=token,
            q=q,
            buying_option="AUCTION",
            limit=MAX_AUCTION_RESULTS_PER_QUERY,
            sort="endingSoonest",
        )

        log(f"items returned: {len(auctions)}")

        for it in auctions:
            seen += 1

            title = (it.get("title") or "").strip()
            if not title:
                continue

            if EXCLUDE_LOTS and looks_like_lot(title):
                continue

            # Light sanity check to avoid random categories
            if not looks_like_card(title):
                continue

            item_id = it.get("itemId")
            if not item_id:
                continue

            total_cost = extract_total_cost(it)
            ends_at = pick_ends_at(it)
            mins = minutes_until(ends_at)

            # If ends_at missing, keep but treat as low priority
            # Still store if profit hits, but most auctions have endTime.

            market, comp_count = estimate_market_from_fixed_price(budget, token, title)

            profit = fee_adjusted_profit(market=market, total_cost=total_cost)

            # Hard filter: only keep big winners
            if profit < MIN_PROFIT:
                continue

            d = Deal(
                item_id=item_id,
                title=title,
                url=pick_url(it),
                image_url=pick_image(it),
                query=q,
                total_cost=round(total_cost, 2),
                market=round(market, 2),
                profit=round(profit, 2),
                ends_at=ends_at,
                minutes_away=mins,
            )

            upsert_deal(engine, d)
            kept += 1

        # Optional: small breather between queries to reduce throttling
        time.sleep(0.8)

    prune_inactive(engine, older_than_hours=72)

    log(f"seen: {seen}")
    log(f"kept: {kept}")
    log("done")


def main() -> int:
    try:
        scan()
        return 0
    except Exception as e:
        # Do not crash the cron. Log and exit clean.
        log(f"fatal: {type(e).__name__}: {e}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
