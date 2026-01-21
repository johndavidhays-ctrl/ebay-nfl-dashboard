import os
import time
import random
import math
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2


SCANNER_VERSION = "HYBRID_MONEY_MODE_V1"

EBAY_CLIENT_ID = os.getenv("EBAY_CLIENT_ID", "").strip()
EBAY_CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

EBAY_OAUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_BROWSE_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

MIN_PROFIT_USD = 150.0

MAX_AUCTION_RESULTS_PER_QUERY = 80
MAX_DEALS_TO_SAVE_PER_RUN = 40

ENDING_WITHIN_MINUTES = 360  # 6 hours
MINUTES_AWAY_CUTOFF = 360

COMP_FIXED_PRICE_LIMIT = 25
MAX_COMP_LOOKUPS_PER_RUN = 35  # keeps API usage sane

SLEEP_BETWEEN_REQUESTS_SEC = 0.65  # quiet scanning
REQUEST_TIMEOUT_SEC = 20

USER_AGENT = "nfl-card-scanner/1.0"


BLOCK_TITLE_SUBSTRINGS = [
    "lot",
    "lots",
    "binder",
    "binders",
    "bulk",
    "pack",
    "packs",
    "hobby box",
    "blaster",
    "break",
    "breaks",
    "team set",
    "complete set",
    "case",
    "random",
    "mystery",
    "replica",
    "reprint",
]

# Queries designed for mispriced singles and short prints without hammering the obvious PSA lanes
AUCTION_QUERIES = [
    "rookie auto /10",
    "rookie autograph /10",
    "ssp rookie auto",
    "case hit rookie",
    "gold vinyl 1/1",
    "black finite 1/1",
    "true black 1/1",
    "gold shimmer /10",
    "gold prizm /10",
    "gold wave /10",
    "blue shimmer /25",
    "gold /10 rookie",
    "on card auto /99 rookie",
    "no huddle ssp",
    "downtown rookie",
    "kaboom rookie",
]

# Used to estimate market using fixed price listings
COMP_QUERY_TWEAKS = [
    "",  # keep as is
    " psa",
    " bgs",
    " sgc",
]


@dataclass
class Deal:
    item_id: str
    title: str
    url: str
    image_url: str
    query: str
    total_cost: float
    market: float
    profit: float
    ends_at: Optional[datetime]
    minutes_away: Optional[int]


def log(msg: str) -> None:
    print(f"SCANNER: {msg}", flush=True)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_ebay_time(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        # eBay returns ISO 8601, usually with Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def minutes_until(dt: Optional[datetime]) -> Optional[int]:
    if not dt:
        return None
    diff = (dt - utc_now()).total_seconds()
    return int(math.floor(diff / 60))


def is_blocked_title(title: str) -> bool:
    t = title.lower()
    for sub in BLOCK_TITLE_SUBSTRINGS:
        if sub in t:
            return True
    return False


def get_access_token() -> str:
    if not EBAY_CLIENT_ID or not EBAY_CLIENT_SECRET:
        raise RuntimeError("Missing EBAY_CLIENT_ID or EBAY_CLIENT_SECRET")

    auth = requests.auth.HTTPBasicAuth(EBAY_CLIENT_ID, EBAY_CLIENT_SECRET)
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": USER_AGENT,
    }

    # Client credentials token
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope",
    }

    r = requests.post(EBAY_OAUTH_URL, headers=headers, data=data, auth=auth, timeout=REQUEST_TIMEOUT_SEC)
    if r.status_code != 200:
        raise RuntimeError(f"Failed to get token: {r.status_code} {r.text}")

    j = r.json()
    token = j.get("access_token")
    if not token:
        raise RuntimeError(f"Token missing in response: {j}")
    return token


def request_with_backoff(method: str, url: str, headers: Dict[str, str], params: Dict[str, Any]) -> Dict[str, Any]:
    max_tries = 7
    base_sleep = 1.2

    for attempt in range(1, max_tries + 1):
        time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

        r = requests.request(method, url, headers=headers, params=params, timeout=REQUEST_TIMEOUT_SEC)

        if r.status_code == 200:
            return r.json()

        # Rate limited
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                try:
                    sleep_s = float(retry_after)
                except Exception:
                    sleep_s = base_sleep * attempt
            else:
                sleep_s = base_sleep * attempt

            # Add jitter
            sleep_s = min(90.0, sleep_s + random.uniform(0.4, 1.6))
            log(f"rate limited 429, sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)
            continue

        # Transient server errors
        if r.status_code in (500, 502, 503, 504):
            sleep_s = min(60.0, base_sleep * attempt + random.uniform(0.4, 1.6))
            log(f"server error {r.status_code}, sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)
            continue

        # Anything else is a real failure
        raise RuntimeError(f"eBay error {r.status_code}: {r.text}")

    raise RuntimeError("eBay request failed after retries")


def ebay_search(
    token: str,
    q: str,
    buying_option: str,
    limit: int,
    sort: str,
) -> List[Dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": USER_AGENT,
    }

    # Auction or Fixed price
    params: Dict[str, Any] = {
        "q": q,
        "limit": limit,
        "sort": sort,
        "filter": f"buyingOptions:{{{buying_option}}}",
    }

    j = request_with_backoff("GET", EBAY_BROWSE_SEARCH_URL, headers=headers, params=params)
    return j.get("itemSummaries", []) or []


def extract_price(item: Dict[str, Any]) -> Tuple[float, float]:
    """
    Returns (item_price, shipping_price)
    """
    price = 0.0
    shipping = 0.0

    p = item.get("price") or {}
    try:
        price = float(p.get("value") or 0.0)
    except Exception:
        price = 0.0

    sp = item.get("shippingOptions") or []
    if sp and isinstance(sp, list):
        first = sp[0] or {}
        shipping_cost = first.get("shippingCost") or {}
        try:
            shipping = float(shipping_cost.get("value") or 0.0)
        except Exception:
            shipping = 0.0

    return price, shipping


def extract_urls(item: Dict[str, Any]) -> Tuple[str, str]:
    url = item.get("itemWebUrl") or ""
    image_url = ""
    img = item.get("image") or {}
    image_url = img.get("imageUrl") or ""
    return url, image_url


def normalize_title_for_comps(title: str) -> str:
    t = title.lower()

    # Strip words that create noisy comp pools
    noise = [
        "🔥",
        "hot",
        "rare",
        "ssp",
        "case hit",
        "invest",
        "look",
        "wow",
        "insane",
        "mint",
        "gem",
        "pop",
        "low pop",
    ]
    for n in noise:
        t = t.replace(n, " ")

    # Reduce whitespace
    t = " ".join(t.split())
    return t[:120]


def estimate_market_from_fixed_price(token: str, title: str) -> Tuple[float, int]:
    """
    Market estimate based on fixed price listings.
    Uses median of prices after trimming outliers.
    Returns (market_estimate, comp_count_used)
    """
    base = normalize_title_for_comps(title)

    prices: List[float] = []

    for tweak in COMP_QUERY_TWEAKS:
        comp_q = (base + tweak).strip()
        items = ebay_search(
            token=token,
            q=comp_q,
            buying_option="FIXED_PRICE",
            limit=COMP_FIXED_PRICE_LIMIT,
            sort="price",  # cheapest first gives better flip signal
        )

        for it in items:
            p, ship = extract_price(it)
            total = float(p) + float(ship)
            if total <= 0:
                continue
            # Ignore absurd comps
            if total > 20000:
                continue
            prices.append(total)

        # Keep the comp calls low
        if len(prices) >= 18:
            break

    if len(prices) < 6:
        return 0.0, len(prices)

    prices.sort()

    # Trim the extreme ends to reduce bad comps
    trim = max(1, int(len(prices) * 0.15))
    core = prices[trim : len(prices) - trim] if len(prices) - 2 * trim >= 3 else prices

    core.sort()
    mid = core[len(core) // 2]
    return float(round(mid, 2)), len(core)


def compute_profit(market: float, total_cost: float) -> float:
    """
    Conservative profit estimate:
    subtract estimated fees and small friction cost.
    """
    if market <= 0:
        return 0.0

    # Rough fees estimate, conservative
    platform_fee = market * 0.135
    friction = 4.0

    profit = market - platform_fee - friction - total_cost
    return float(round(profit, 2))


def connect_db():
    if not DATABASE_URL:
        raise RuntimeError("Missing DATABASE_URL")
    return psycopg2.connect(DATABASE_URL)


def ensure_schema(conn) -> None:
    """
    Ensures the dashboard will not crash by guaranteeing deals.id exists.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS deals (
                id BIGSERIAL,
                item_id TEXT,
                title TEXT,
                url TEXT,
                image_url TEXT,
                query TEXT,
                total_cost DOUBLE PRECISION,
                market DOUBLE PRECISION,
                profit DOUBLE PRECISION,
                ends_at TIMESTAMPTZ,
                minutes_away INTEGER,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )

        # Add id if table existed without it
        cur.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name='deals' AND column_name='id'
                ) THEN
                    ALTER TABLE deals ADD COLUMN id BIGSERIAL;
                END IF;
            END $$;
            """
        )

        # Ensure item_id exists
        cur.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name='deals' AND column_name='item_id'
                ) THEN
                    ALTER TABLE deals ADD COLUMN item_id TEXT;
                END IF;
            END $$;
            """
        )

        # Ensure a unique constraint on item_id so upserts work
        cur.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_indexes
                    WHERE tablename='deals' AND indexname='deals_item_id_uidx'
                ) THEN
                    CREATE UNIQUE INDEX deals_item_id_uidx ON deals(item_id);
                END IF;
            END $$;
            """
        )

        conn.commit()


def mark_all_inactive(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE deals SET is_active = FALSE, updated_at = NOW() WHERE is_active = TRUE;")
    conn.commit()


def upsert_deal(conn, d: Deal) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO deals (
                item_id, title, url, image_url, query,
                total_cost, market, profit, ends_at, minutes_away,
                is_active, created_at, updated_at
            )
            VALUES (
                %(item_id)s, %(title)s, %(url)s, %(image_url)s, %(query)s,
                %(total_cost)s, %(market)s, %(profit)s, %(ends_at)s, %(minutes_away)s,
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
                minutes_away = EXCLUDED.minutes_away,
                is_active = TRUE,
                updated_at = NOW();
            """,
            {
                "item_id": d.item_id,
                "title": d.title,
                "url": d.url,
                "image_url": d.image_url,
                "query": d.query,
                "total_cost": d.total_cost,
                "market": d.market,
                "profit": d.profit,
                "ends_at": d.ends_at,
                "minutes_away": d.minutes_away,
            },
        )
    conn.commit()


def scan() -> None:
    log(f"SCANNER VERSION: {SCANNER_VERSION}")

    token = get_access_token()

    conn = connect_db()
    ensure_schema(conn)

    # Reset actives each run, then re activate what we still like
    mark_all_inactive(conn)

    kept: List[Deal] = []
    comp_lookups = 0

    # Pull auctions in ending soon order, then filter for minutes away ourselves
    for q in AUCTION_QUERIES:
        if len(kept) >= MAX_DEALS_TO_SAVE_PER_RUN:
            break

        log(f"query: {q}")
        auctions = ebay_search(
            token=token,
            q=q,
            buying_option="AUCTION",
            limit=MAX_AUCTION_RESULTS_PER_QUERY,
            sort="endingSoonest",
        )
        log(f"items returned: {len(auctions)}")

        for item in auctions:
            if len(kept) >= MAX_DEALS_TO_SAVE_PER_RUN:
                break

            title = (item.get("title") or "").strip()
            if not title:
                continue

            if is_blocked_title(title):
                continue

            item_id = item.get("itemId") or ""
            if not item_id:
                continue

            ends_at = parse_ebay_time((item.get("itemEndDate") or item.get("itemEndDateTime") or item.get("estimatedEndTime")))
            if not ends_at:
                # browse sometimes omits, skip
                continue

            mins = minutes_until(ends_at)
            if mins is None:
                continue

            if mins < 0 or mins > MINUTES_AWAY_CUTOFF:
                continue

            price, ship = extract_price(item)
            total_cost = float(round(price + ship, 2))

            # quick sanity filters so we do not burn comp calls on junk
            if total_cost <= 0:
                continue
            if total_cost > 2000:
                continue

            # We only do comp lookups on candidates that could plausibly hit profit floor.
            # Assume fees plus friction around 18 percent, so market must beat cost by margin.
            rough_needed_market = total_cost + MIN_PROFIT_USD + (total_cost * 0.15) + 10.0
            if rough_needed_market > 3500:
                continue

            if comp_lookups >= MAX_COMP_LOOKUPS_PER_RUN:
                continue

            market, comp_count = estimate_market_from_fixed_price(token, title)
            comp_lookups += 1

            if market <= 0 or comp_count < 6:
                continue

            profit = compute_profit(market=market, total_cost=total_cost)
            if profit < MIN_PROFIT_USD:
                continue

            url, image_url = extract_urls(item)

            d = Deal(
                item_id=item_id,
                title=title,
                url=url,
                image_url=image_url,
                query=q,
                total_cost=total_cost,
                market=market,
                profit=profit,
                ends_at=ends_at,
                minutes_away=mins,
            )

            kept.append(d)

    # Sort by nearest ending, then highest profit
    kept.sort(key=lambda d: ((d.minutes_away if d.minutes_away is not None else 10**9), -d.profit))

    saved = 0
    for d in kept:
        if saved >= MAX_DEALS_TO_SAVE_PER_RUN:
            break
        upsert_deal(conn, d)
        saved += 1

    conn.close()

    log(f"seen queries: {len(AUCTION_QUERIES)}")
    log(f"comp lookups used: {comp_lookups}")
    log(f"kept: {len(kept)}")
    log(f"saved: {saved}")


if __name__ == "__main__":
    scan()
