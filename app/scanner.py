# scanner.py
# Auction focused deal scanner for sports card singles on eBay
# Finds mispriced auctions ending soon, scores them, estimates profit using sold comps when possible

import os
import re
import time
import json
import math
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    import psycopg2
    from psycopg2.extras import execute_values
except Exception:
    psycopg2 = None


logging.basicConfig(level=logging.INFO, format="SCANNER: %(message)s")
log = logging.getLogger("scanner")


SCANNER_VERSION = "AUCTIONS_SINGLES_ENDING_SOON_STEALS_V1"


# =========================
# Tuning knobs
# =========================

# What you told me you want
MAX_PRICE_USD = float(os.getenv("MAX_PRICE_USD", "150"))          # focus on cheaper auctions
MIN_BUY_PRICE_USD = float(os.getenv("MIN_BUY_PRICE_USD", "10"))   # ignore ultra cheap noise
MAX_ITEM_COUNT_PER_QUERY = int(os.getenv("MAX_ITEM_COUNT_PER_QUERY", "200"))

# Ending soon window
ENDING_SOON_HOURS = float(os.getenv("ENDING_SOON_HOURS", "48"))

# Profit focus
MIN_EST_PROFIT_USD = float(os.getenv("MIN_EST_PROFIT_USD", "10"))  # start here, raise to 100 when you want fewer
MIN_ROI = float(os.getenv("MIN_ROI", "0.8"))                       # 0.8 means 80%+ ROI
MAX_TOTAL_RESULTS_TO_SAVE = int(os.getenv("MAX_TOTAL_RESULTS_TO_SAVE", "250"))

# Fees model
EBAY_FEE_RATE = float(os.getenv("EBAY_FEE_RATE", "0.1325"))        # rough blended fee rate
EBAY_ORDER_FIXED_FEE = float(os.getenv("EBAY_ORDER_FIXED_FEE", "0.30"))

# Market value estimation strategy
# If sold comps query fails, fallback uses a conservative multiplier based on rarity signals
FALLBACK_BASE_MULTIPLIER = float(os.getenv("FALLBACK_BASE_MULTIPLIER", "1.6"))

# Search behavior
COUNTRY = os.getenv("COUNTRY", "US")
CATEGORY_IDS = os.getenv("CATEGORY_IDS", "")  # optional comma list, leave blank to let query roam

# eBay auth
EBAY_OAUTH_TOKEN = os.getenv("EBAY_OAUTH_TOKEN", "").strip()
EBAY_MARKETPLACE_ID = os.getenv("EBAY_MARKETPLACE_ID", "EBAY_US")

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


# =========================
# Queries: use broad, messy, human language queries
# =========================

DEFAULT_QUERIES = [
    "sports card",
    "rookie card",
    "autograph card",
    "patch card",
    "numbered card",
    "refractor card",
    "parallel card",
    "short print card",
    "ssp",
    "sp",
    "silver prizm",
    "gold /",
    "color match",
    "case hit",
]

# If you want to focus NFL only, set SPORTS_KEYWORDS in env, example: "football,nfl"
SPORTS_KEYWORDS = [s.strip().lower() for s in os.getenv("SPORTS_KEYWORDS", "").split(",") if s.strip()]


# =========================
# Helpers
# =========================

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def parse_iso_dt(s: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def hours_until(dt: datetime) -> float:
    return (dt - now_utc()).total_seconds() / 3600.0


def money_round(x: float) -> float:
    return float(f"{x:.2f}")


def title_has_sports_focus(title: str) -> bool:
    if not SPORTS_KEYWORDS:
        return True
    t = title.lower()
    return any(k in t for k in SPORTS_KEYWORDS)


def build_filter_for_auctions(max_price: float) -> str:
    # eBay Browse API filter format
    # We are targeting auctions and keeping price low
    # End time filter is not officially supported in all regions, so we filter by end time in code
    parts = [
        "buyingOptions:{AUCTION}",
        f"price:[{MIN_BUY_PRICE_USD}..{max_price}]",
        "priceCurrency:USD",
        f"itemLocationCountry:{COUNTRY}",
    ]
    return ",".join(parts)


def build_headers() -> Dict[str, str]:
    if not EBAY_OAUTH_TOKEN:
        raise RuntimeError("Missing EBAY_OAUTH_TOKEN in environment")
    return {
        "Authorization": f"Bearer {EBAY_OAUTH_TOKEN}",
        "Content-Type": "application/json",
        "X-EBAY-C-MARKETPLACE-ID": EBAY_MARKETPLACE_ID,
    }


def build_search_params(q: str, limit: int, offset: int) -> Dict[str, str]:
    params = {
        "q": q,
        "limit": str(limit),
        "offset": str(offset),
        "filter": build_filter_for_auctions(MAX_PRICE_USD),
    }
    if CATEGORY_IDS.strip():
        params["category_ids"] = CATEGORY_IDS.strip()
    return params


def ebay_browse_search(q: str, limit: int = 50, offset: int = 0) -> Dict[str, Any]:
    url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
    r = requests.get(url, headers=build_headers(), params=build_search_params(q, limit, offset), timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"browse search failed {r.status_code}: {r.text[:500]}")
    return r.json()


def ebay_browse_sold_comps(q: str, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
    """
    Attempts sold comps using Browse API.
    Not all accounts have access to sold items filters. If it fails, return None.
    """
    url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
    params = {
        "q": q,
        "limit": str(limit),
        "offset": "0",
        "filter": f"soldItems,priceCurrency:USD,itemLocationCountry:{COUNTRY}",
    }
    if CATEGORY_IDS.strip():
        params["category_ids"] = CATEGORY_IDS.strip()

    try:
        r = requests.get(url, headers=build_headers(), params=params, timeout=30)
        if r.status_code >= 400:
            return None
        data = r.json()
        items = data.get("itemSummaries") or []
        return items
    except Exception:
        return None


def extract_price_and_ship(item: Dict[str, Any]) -> Tuple[float, float]:
    price = 0.0
    ship = 0.0

    p = item.get("price") or {}
    price = safe_float(p.get("value"), 0.0)

    s = item.get("shippingOptions") or []
    if s and isinstance(s, list):
        # pick the cheapest shipping option if present
        best = None
        for opt in s:
            c = opt.get("shippingCost") or {}
            v = safe_float(c.get("value"), None)
            if v is None:
                continue
            if best is None or v < best:
                best = v
        if best is not None:
            ship = float(best)

    # sometimes it is in item.get("shippingCost")
    if ship == 0.0:
        c = item.get("shippingCost") or {}
        ship = safe_float(c.get("value"), 0.0)

    return price, ship


def extract_end_time(item: Dict[str, Any]) -> Optional[datetime]:
    # Auctions usually have "itemEndDate" in the browse summary
    end_s = item.get("itemEndDate") or item.get("itemEndTime") or ""
    if not end_s:
        return None
    return parse_iso_dt(end_s)


def extract_bid_count(item: Dict[str, Any]) -> int:
    bc = item.get("bidCount")
    try:
        return int(bc) if bc is not None else 0
    except Exception:
        return 0


def rarity_signals(title: str) -> Dict[str, Any]:
    t = title.lower()

    signals = {
        "auto": 1 if ("auto" in t or "autograph" in t or "on card" in t) else 0,
        "patch": 1 if ("patch" in t or "jersey" in t or "relic" in t or "game used" in t) else 0,
        "numbered": 1 if (re.search(r"/\s*\d{1,3}\b", t) is not None or "numbered" in t) else 0,
        "refractor": 1 if ("refractor" in t or "prizm" in t or "holo" in t) else 0,
        "short_print": 1 if ("ssp" in t or "sp" in t or "short print" in t) else 0,
        "color": 1 if ("gold" in t or "orange" in t or "green" in t or "blue" in t or "red" in t) else 0,
        "case_hit": 1 if ("case hit" in t or "downtown" in t or "kaboom" in t or "genesis" in t) else 0,
        "rookie": 1 if ("rookie" in t or "rc" in t) else 0,
        "graded": 1 if ("psa" in t or "bgs" in t or "sgc" in t) else 0,
    }

    # pull serial like /99
    serial = None
    m = re.search(r"/\s*(\d{1,3})\b", t)
    if m:
        serial = int(m.group(1))
    signals["serial"] = serial

    return signals


def compute_fallback_multiplier(signals: Dict[str, Any]) -> float:
    mult = FALLBACK_BASE_MULTIPLIER

    if signals.get("case_hit"):
        mult += 1.3
    if signals.get("auto"):
        mult += 0.6
    if signals.get("patch"):
        mult += 0.5
    if signals.get("short_print"):
        mult += 0.4
    if signals.get("refractor"):
        mult += 0.25
    if signals.get("color"):
        mult += 0.15
    if signals.get("numbered"):
        mult += 0.35
    if signals.get("rookie"):
        mult += 0.2

    serial = signals.get("serial")
    if isinstance(serial, int) and serial > 0:
        if serial <= 10:
            mult += 1.0
        elif serial <= 25:
            mult += 0.7
        elif serial <= 50:
            mult += 0.45
        elif serial <= 99:
            mult += 0.25
        else:
            mult += 0.1

    # graded can be less mispriced in auctions, reduce a bit to keep focus on raw steals
    if signals.get("graded"):
        mult -= 0.15

    return max(1.1, mult)


def estimate_market_value(title: str, buy_price: float, ship: float) -> Tuple[Optional[float], Optional[float], str]:
    """
    Returns (market_value, comp_median, method)
    market_value is what we think it can sell for
    comp_median is median sold comp if we could fetch it
    method is "sold_comps" or "fallback"
    """
    q = title

    comps = ebay_browse_sold_comps(q, limit=40)
    prices = []
    if comps:
        for it in comps:
            p = it.get("price") or {}
            v = safe_float(p.get("value"), None)
            if v is None:
                continue
            if v <= 0:
                continue
            prices.append(v)

    if prices:
        prices.sort()
        mid = prices[len(prices) // 2]
        # take a haircut because your listing will not always hit median
        market = mid * 0.92
        return money_round(market), money_round(mid), "sold_comps"

    # fallback
    sig = rarity_signals(title)
    mult = compute_fallback_multiplier(sig)
    market = (buy_price + ship) * mult
    return money_round(market), None, "fallback"


def compute_profit_and_roi(market_value: float, buy: float, ship: float) -> Tuple[float, float]:
    gross = market_value
    fees = gross * EBAY_FEE_RATE + EBAY_ORDER_FIXED_FEE
    net = gross - fees
    cost = buy + ship
    profit = net - cost
    roi = profit / cost if cost > 0 else 0.0
    return money_round(profit), money_round(roi)


def compute_score(profit: float, roi: float, hours_left: float, bid_count: int, signals: Dict[str, Any]) -> float:
    # This score is designed to bubble up auctions that are:
    # big upside, low competition, ending soon, with rarity signals
    rarity = (
        signals.get("case_hit", 0) * 35
        + signals.get("auto", 0) * 18
        + signals.get("patch", 0) * 14
        + signals.get("numbered", 0) * 12
        + signals.get("short_print", 0) * 10
        + signals.get("refractor", 0) * 6
        + signals.get("rookie", 0) * 5
        + signals.get("color", 0) * 3
    )

    serial = signals.get("serial")
    if isinstance(serial, int) and serial > 0:
        if serial <= 10:
            rarity += 18
        elif serial <= 25:
            rarity += 12
        elif serial <= 50:
            rarity += 7
        elif serial <= 99:
            rarity += 4

    # lower bids is better
    competition = max(0.0, 18.0 - min(bid_count, 18))

    # ending sooner is better, but not too soon
    # up to 48 hours, we give more weight as it gets closer
    time_factor = 0.0
    if hours_left <= 0:
        time_factor = -20.0
    else:
        # 48 hours left => small bump, 4 hours left => bigger bump
        time_factor = max(0.0, 22.0 - (hours_left / (ENDING_SOON_HOURS / 22.0)))

    # Profit dominates, but ROI and signals also matter
    score = (
        profit * 0.9
        + roi * 60.0
        + rarity
        + competition
        + time_factor
    )

    return float(f"{score:.2f}")


def build_links(item: Dict[str, Any]) -> Dict[str, str]:
    url = item.get("itemWebUrl") or ""
    sold_search_url = ""
    if url:
        sold_search_url = "https://www.ebay.com/sch/i.html?_nkw=" + requests.utils.quote(item.get("title", "")) + "&LH_Sold=1&LH_Complete=1"
    return {"listing": url, "sold_comps": sold_search_url}


def normalize_row(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    title = item.get("title") or ""
    if not title:
        return None
    if not title_has_sports_focus(title):
        return None

    buy, ship = extract_price_and_ship(item)
    if buy <= 0:
        return None
    if buy < MIN_BUY_PRICE_USD or buy > MAX_PRICE_USD:
        return None

    end_dt = extract_end_time(item)
    if not end_dt:
        return None

    hrs = hours_until(end_dt)
    if hrs <= 0 or hrs > ENDING_SOON_HOURS:
        return None

    bid_count = extract_bid_count(item)

    sig = rarity_signals(title)

    market_value, comp_median, method = estimate_market_value(title, buy, ship)
    if market_value is None or market_value <= 0:
        return None

    est_profit, roi = compute_profit_and_roi(market_value, buy, ship)

    if est_profit < MIN_EST_PROFIT_USD:
        return None
    if roi < MIN_ROI:
        return None

    score = compute_score(est_profit, roi, hrs, bid_count, sig)

    links = build_links(item)

    # try to detect listing type
    listing_type = "auction"

    row = {
        "title": title,
        "listing_type": listing_type,
        "buy_price": money_round(buy),
        "ship": money_round(ship),
        "market_value": money_round(market_value),
        "comp_median": money_round(comp_median) if comp_median is not None else None,
        "est_profit": money_round(est_profit),
        "roi": money_round(roi),
        "score": score,
        "bid_count": bid_count,
        "end_time": end_dt.isoformat(),
        "item_id": item.get("itemId"),
        "item_url": links["listing"],
        "sold_comps_url": links["sold_comps"],
        "est_method": method,
        "signals": sig,
    }
    return row


# =========================
# Database helpers
# =========================

def db_connect():
    if not DATABASE_URL:
        return None
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not installed but DATABASE_URL is set")
    return psycopg2.connect(DATABASE_URL)


def db_ensure_schema(conn) -> None:
    # This is defensive: adds the columns you have been working with
    # and creates the table if it does not exist.
    ddl = """
    CREATE TABLE IF NOT EXISTS deals (
      item_id text PRIMARY KEY,
      title text,
      listing_type text,
      buy_price numeric,
      ship numeric,
      market_value numeric,
      comp_median numeric,
      est_profit numeric,
      roi numeric,
      score numeric,
      bid_count integer,
      end_time timestamptz,
      item_url text,
      sold_comps_url text,
      est_method text,
      signals jsonb,
      created_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now()
    );

    ALTER TABLE deals
      ADD COLUMN IF NOT EXISTS listing_type text;

    ALTER TABLE deals
      ADD COLUMN IF NOT EXISTS market_value numeric;

    ALTER TABLE deals
      ADD COLUMN IF NOT EXISTS comp_median numeric;

    ALTER TABLE deals
      ADD COLUMN IF NOT EXISTS est_profit numeric;

    ALTER TABLE deals
      ADD COLUMN IF NOT EXISTS roi numeric;

    ALTER TABLE deals
      ADD COLUMN IF NOT EXISTS score numeric;

    ALTER TABLE deals
      ADD COLUMN IF NOT EXISTS bid_count integer;

    ALTER TABLE deals
      ADD COLUMN IF NOT EXISTS end_time timestamptz;

    ALTER TABLE deals
      ADD COLUMN IF NOT EXISTS item_url text;

    ALTER TABLE deals
      ADD COLUMN IF NOT EXISTS sold_comps_url text;

    ALTER TABLE deals
      ADD COLUMN IF NOT EXISTS est_method text;

    ALTER TABLE deals
      ADD COLUMN IF NOT EXISTS signals jsonb;

    ALTER TABLE deals
      ADD COLUMN IF NOT EXISTS created_at timestamptz;

    ALTER TABLE deals
      ADD COLUMN IF NOT EXISTS updated_at timestamptz;
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def db_upsert_rows(conn, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    cols = [
        "item_id",
        "title",
        "listing_type",
        "buy_price",
        "ship",
        "market_value",
        "comp_median",
        "est_profit",
        "roi",
        "score",
        "bid_count",
        "end_time",
        "item_url",
        "sold_comps_url",
        "est_method",
        "signals",
        "updated_at",
    ]

    values = []
    for r in rows:
        values.append((
            r["item_id"],
            r["title"],
            r["listing_type"],
            r["buy_price"],
            r["ship"],
            r["market_value"],
            r["comp_median"],
            r["est_profit"],
            r["roi"],
            r["score"],
            r["bid_count"],
            r["end_time"],
            r["item_url"],
            r["sold_comps_url"],
            r["est_method"],
            json.dumps(r["signals"]),
            now_utc().isoformat(),
        ))

    sql = f"""
      INSERT INTO deals ({",".join(cols)})
      VALUES %s
      ON CONFLICT (item_id) DO UPDATE SET
        title = EXCLUDED.title,
        listing_type = EXCLUDED.listing_type,
        buy_price = EXCLUDED.buy_price,
        ship = EXCLUDED.ship,
        market_value = EXCLUDED.market_value,
        comp_median = EXCLUDED.comp_median,
        est_profit = EXCLUDED.est_profit,
        roi = EXCLUDED.roi,
        score = EXCLUDED.score,
        bid_count = EXCLUDED.bid_count,
        end_time = EXCLUDED.end_time,
        item_url = EXCLUDED.item_url,
        sold_comps_url = EXCLUDED.sold_comps_url,
        est_method = EXCLUDED.est_method,
        signals = EXCLUDED.signals,
        updated_at = EXCLUDED.updated_at
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=200)
    conn.commit()
    return len(rows)


def db_prune_old(conn) -> int:
    # Remove ended auctions so your list naturally clears itself
    # This prevents stale results from hanging around
    with conn.cursor() as cur:
        cur.execute("DELETE FROM deals WHERE end_time IS NOT NULL AND end_time < now();")
        deleted = cur.rowcount
    conn.commit()
    return deleted


# =========================
# Main scan loop
# =========================

def run():
    log.info(f"SCANNER VERSION: {SCANNER_VERSION}")

    if not EBAY_OAUTH_TOKEN:
        raise RuntimeError("Set EBAY_OAUTH_TOKEN in your environment")

    queries = [q.strip() for q in os.getenv("QUERIES", "").split("|") if q.strip()]
    if not queries:
        queries = DEFAULT_QUERIES

    total_seen = 0
    kept: List[Dict[str, Any]] = []

    for q in queries:
        log.info(f"query: {q}")

        offset = 0
        while offset < MAX_ITEM_COUNT_PER_QUERY:
            limit = min(50, MAX_ITEM_COUNT_PER_QUERY - offset)
            data = ebay_browse_search(q, limit=limit, offset=offset)

            items = data.get("itemSummaries") or []
            if not items:
                break

            total_seen += len(items)

            for item in items:
                row = normalize_row(item)
                if row:
                    kept.append(row)

            # stop early if we are already flooded
            if len(kept) >= MAX_TOTAL_RESULTS_TO_SAVE * 2:
                break

            offset += limit

            # gentle pacing
            time.sleep(0.2)

        log.info(f"items_seen_so_far: {total_seen}, kept_so_far: {len(kept)}")

        if len(kept) >= MAX_TOTAL_RESULTS_TO_SAVE * 2:
            break

    # sort and cap
    kept.sort(key=lambda r: (r["score"], r["est_profit"]), reverse=True)
    kept = kept[:MAX_TOTAL_RESULTS_TO_SAVE]

    # print summary
    log.info(f"total_seen: {total_seen}")
    log.info(f"total_kept: {len(kept)}")

    if DATABASE_URL:
        conn = db_connect()
        if conn:
            db_ensure_schema(conn)
            pruned = db_prune_old(conn)
            if pruned:
                log.info(f"pruned_ended: {pruned}")
            inserted = db_upsert_rows(conn, kept)
            log.info(f"upserted: {inserted}")
            conn.close()
    else:
        # no DB configured, dump top results so you can still see output in logs
        top = kept[:20]
        log.info("top_results:")
        for r in top:
            log.info(f"score={r['score']} profit=${r['est_profit']} roi={int(r['roi']*100)}% buy=${r['buy_price']} ship=${r['ship']} bids={r['bid_count']} ends={r['end_time']} title={r['title'][:120]}")

    return kept


if __name__ == "__main__":
    run()
