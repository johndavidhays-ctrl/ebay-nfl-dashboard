# app/scanner.py
import os
import sys
import time
import math
import json
import traceback
from datetime import datetime, timezone, timedelta

import requests

try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None


SCANNER_VERSION = "AUCTIONS_SINGLES_FAST_PROFIT_V1"


def log(msg: str) -> None:
    ts = datetime.now().strftime("%b %d %I:%M:%S %p")
    print(f"{ts}  SCANNER: {msg}", flush=True)


def getenv_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return v.strip()


def getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default


def getenv_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ebay_headers(oauth_token: str) -> dict:
    return {
        "Authorization": f"Bearer {oauth_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def http_get(url: str, headers: dict, params: dict) -> requests.Response:
    return requests.get(url, headers=headers, params=params, timeout=30)


def parse_money(value) -> float:
    try:
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        s = str(value).strip().replace("$", "").replace(",", "")
        return float(s)
    except Exception:
        return 0.0


def safe_get(d: dict, path: str, default=None):
    cur = d
    for p in path.split("."):
        if not isinstance(cur, dict):
            return default
        if p not in cur:
            return default
        cur = cur[p]
    return cur


def listing_type_from_item(item: dict) -> str:
    buying = item.get("buyingOptions") or []
    if "AUCTION" in buying:
        return "AUCTION"
    if "FIXED_PRICE" in buying:
        return "FIXED_PRICE"
    if "BEST_OFFER" in buying:
        return "BEST_OFFER"
    return (buying[0] if buying else "") or ""


def extract_prices(item: dict) -> tuple[float, float]:
    price = parse_money(safe_get(item, "price.value", 0.0))
    ship = parse_money(safe_get(item, "shippingOptions.0.shippingCost.value", 0.0))
    return price, ship


def end_time_utc(item: dict):
    t = safe_get(item, "itemEndDate", None) or safe_get(item, "itemEndDateTime", None)
    if not t:
        return None
    try:
        return datetime.fromisoformat(t.replace("Z", "+00:00"))
    except Exception:
        return None


def title_text(item: dict) -> str:
    return (item.get("title") or "").strip()


def seller_feedback_percent(item: dict):
    v = safe_get(item, "seller.feedbackPercentage", None)
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def build_queries() -> list[str]:
    custom = getenv_str("SCAN_QUERIES", "")
    if custom:
        return [q.strip() for q in custom.split("|") if q.strip()]

    # Default: broad but designed to catch undervalued singles
    return [
        "sports card",
        "rookie card",
        "autograph card",
        "numbered card",
        "refractor card",
        "parallel card",
        "short print card",
        "ssp sports card",
        "sp sports card",
        "case hit sports card",
        "holo rookie card",
    ]


def should_exclude_title(t: str) -> bool:
    tl = t.lower()

    # Singles only, avoid lots
    lot_words = [
        "lot of", "card lot", "lots", "bundle", "collection", "set", "pick your",
        "you pick", "choose", "bulk", "random", "mystery", "break", "team lot",
    ]
    for w in lot_words:
        if w in tl:
            return True

    # Avoid obvious non cards
    non_card = ["pack", "box", "case", "blaster", "hobby", "mega box", "fat pack"]
    for w in non_card:
        if w in tl:
            return True

    return False


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def estimate_fees(sale_price: float) -> float:
    # Rough all in: ebay final value + payment processing
    # You can override with EBAY_FEE_RATE
    fee_rate = getenv_float("EBAY_FEE_RATE", 0.135)
    return sale_price * fee_rate


def compute_profit(
    buy_price: float,
    ship_price: float,
    est_sale: float,
    out_ship: float,
) -> tuple[float, float]:
    cost = buy_price + ship_price
    fees = estimate_fees(est_sale)
    profit = est_sale - cost - fees - out_ship
    roi = 0.0
    if cost > 0:
        roi = profit / cost
    return profit, roi


def score_deal(profit: float, roi: float, hours_left: float, feedback_pct, price: float) -> float:
    # Higher profit and ROI is better, ending sooner is better, low price gets a small bonus
    # Seller feedback helps reduce junk
    profit_component = clamp(profit / 200.0, 0.0, 3.0)
    roi_component = clamp(roi * 2.0, 0.0, 3.0)

    urgency = 0.0
    if hours_left is not None:
        urgency = clamp((24.0 - hours_left) / 24.0, 0.0, 1.0) * 1.2

    trust = 0.0
    if feedback_pct is not None:
        trust = clamp((feedback_pct - 95.0) / 5.0, 0.0, 1.0) * 0.8

    price_bonus = 0.0
    if price > 0:
        price_bonus = clamp((200.0 - price) / 200.0, 0.0, 1.0) * 0.5

    return profit_component + roi_component + urgency + trust + price_bonus


def db_connect():
    db_url = getenv_str("DATABASE_URL", "")
    if not db_url:
        return None
    if psycopg2 is None:
        raise RuntimeError("DATABASE_URL is set but psycopg2 is not installed. Add psycopg2-binary to requirements.txt.")
    return psycopg2.connect(db_url)


def db_ensure_schema(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS deals (
            item_id text PRIMARY KEY,
            title text,
            listing_url text,
            sold_comps_url text,
            listing_type text,
            buy_price numeric,
            ship_price numeric,
            est_sale numeric,
            est_profit numeric,
            roi numeric,
            score numeric,
            end_time timestamptz,
            seller_feedback_percent numeric,
            active boolean DEFAULT TRUE,
            created_at timestamptz DEFAULT now(),
            last_seen timestamptz DEFAULT now()
        );
        """
    )

    cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS title text;")
    cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS listing_url text;")
    cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS sold_comps_url text;")
    cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS listing_type text;")
    cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS buy_price numeric;")
    cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS ship_price numeric;")
    cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS est_sale numeric;")
    cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS est_profit numeric;")
    cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS roi numeric;")
    cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS score numeric;")
    cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS end_time timestamptz;")
    cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS seller_feedback_percent numeric;")
    cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS active boolean DEFAULT TRUE;")
    cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now();")
    cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS last_seen timestamptz DEFAULT now();")


def db_mark_all_inactive(cur):
    cur.execute("UPDATE deals SET active = FALSE;")


def db_upsert_deal(cur, deal: dict):
    cur.execute(
        """
        INSERT INTO deals (
            item_id, title, listing_url, sold_comps_url, listing_type,
            buy_price, ship_price, est_sale, est_profit, roi, score,
            end_time, seller_feedback_percent, active, last_seen
        )
        VALUES (
            %(item_id)s, %(title)s, %(listing_url)s, %(sold_comps_url)s, %(listing_type)s,
            %(buy_price)s, %(ship_price)s, %(est_sale)s, %(est_profit)s, %(roi)s, %(score)s,
            %(end_time)s, %(seller_feedback_percent)s, TRUE, now()
        )
        ON CONFLICT (item_id) DO UPDATE SET
            title = EXCLUDED.title,
            listing_url = EXCLUDED.listing_url,
            sold_comps_url = EXCLUDED.sold_comps_url,
            listing_type = EXCLUDED.listing_type,
            buy_price = EXCLUDED.buy_price,
            ship_price = EXCLUDED.ship_price,
            est_sale = EXCLUDED.est_sale,
            est_profit = EXCLUDED.est_profit,
            roi = EXCLUDED.roi,
            score = EXCLUDED.score,
            end_time = EXCLUDED.end_time,
            seller_feedback_percent = EXCLUDED.seller_feedback_percent,
            active = TRUE,
            last_seen = now();
        """,
        deal,
    )


def db_prune_inactive(cur):
    # Remove anything not seen in the most recent run
    cur.execute("DELETE FROM deals WHERE active = FALSE;")


def sold_comps_url_for(title: str) -> str:
    # Simple link to ebay sold search page in the browser
    q = requests.utils.quote(title)
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_Sold=1&LH_Complete=1"


def estimate_sale_price_via_marketplace_insights(token: str, title: str) -> float | None:
    # This endpoint requires buy.marketplace.insights scope.
    # If your token does not have it, this will 401 or 403.
    url = "https://api.ebay.com/buy/marketplace_insights/v1/item_sales/search"
    params = {
        "q": title,
        "limit": "50",
        "sort": "saleDate",
        "filter": "soldDate:[now-30d..now]",
    }
    r = http_get(url, ebay_headers(token), params)
    if r.status_code != 200:
        return None
    data = r.json()
    items = data.get("itemSales") or []
    prices = []
    for it in items:
        p = parse_money(safe_get(it, "price.value", None))
        if p > 0:
            prices.append(p)
    if not prices:
        return None

    prices.sort()
    # Use median to reduce outliers
    mid = len(prices) // 2
    if len(prices) % 2 == 1:
        return float(prices[mid])
    return float((prices[mid - 1] + prices[mid]) / 2.0)


def browse_search(token: str, q: str, limit: int, offset: int, filters: str, sort: str) -> dict | None:
    url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
    params = {
        "q": q,
        "limit": str(limit),
        "offset": str(offset),
        "sort": sort,
        "filter": filters,
    }
    r = http_get(url, ebay_headers(token), params)
    log(f"BROWSE STATUS: {r.status_code}")
    if r.status_code != 200:
        try:
            log(f"BROWSE BODY: {r.text[:500]}")
        except Exception:
            pass
        return None
    return r.json()


def run():
    log(f"SCANNER VERSION: {SCANNER_VERSION}")

    token = getenv_str("EBAY_OAUTH_TOKEN", "")
    if not token:
        raise RuntimeError("EBAY_OAUTH_TOKEN is missing in Render Environment for nfl-card-scanner")

    # Targets
    max_buy_price = getenv_float("MAX_BUY_PRICE", 150.0)     # focus on cheap auctions
    min_buy_price = getenv_float("MIN_BUY_PRICE", 10.0)      # avoid junk below 10
    max_total_price = getenv_float("MAX_TOTAL_PRICE", 200.0) # buy+ship cap
    max_end_hours = getenv_float("MAX_END_HOURS", 8.0)       # auctions ending soon
    min_profit = getenv_float("MIN_EST_PROFIT", 100.0)       # your current goal
    min_profit_floor = getenv_float("MIN_EST_PROFIT_FLOOR", 10.0)  # fallback if you want more volume
    require_min_profit_floor = getenv_int("REQUIRE_MIN_PROFIT_FLOOR", 1)  # 1 means also keep 10+ profit if it meets rules
    out_ship = getenv_float("OUTBOUND_SHIP_COST", 5.0)       # what you pay to ship to buyer later
    max_results_per_query = getenv_int("MAX_ITEMS_PER_QUERY", 200)
    page_limit = getenv_int("MAX_PAGES_PER_QUERY", 2)        # each page is 200 max
    region = getenv_str("EBAY_MARKETPLACE_ID", "EBAY_US")

    # Filters
    end_to = now_utc() + timedelta(hours=max_end_hours)
    end_from = now_utc()

    # Browse API uses marketplaceId header, but it can also accept via param in some setups.
    # We will send it via header as well by extending headers at call sites.
    # For simplicity we keep headers in ebay_headers; Marketplace header is supported.
    # If your account requires it, set EBAY_MARKETPLACE_ID.
    base_headers = ebay_headers(token)
    base_headers["X-EBAY-C-MARKETPLACE-ID"] = region

    conn = None
    cur = None
    db_enabled = False

    db_url = getenv_str("DATABASE_URL", "")
    if db_url:
        conn = db_connect()
        conn.autocommit = True
        cur = conn.cursor()
        db_ensure_schema(cur)
        db_mark_all_inactive(cur)
        db_enabled = True
        log("db: connected, schema ensured, marked all inactive")
    else:
        log("db: DATABASE_URL not set, running without database writes")

    queries = build_queries()
    log(f"queries: {len(queries)}")

    total_seen = 0
    total_kept = 0
    total_inserted = 0

    # Auction only, singles only, ending soon
    # Filters reference: buyingOptions and price ranges are supported in Browse search.
    # endTime filter is supported via itemEndDate for auctions, but syntax varies.
    # We will still enforce end time in code to be safe.
    for q in queries:
        log(f"query: {q}")

        # Auction only, US, sports cards category can be too narrow, we keep broad but enforce title rules
        filters = [
            "buyingOptions:{AUCTION}",
            f"price:[{min_buy_price}..{max_buy_price}]",
        ]

        # You can optionally filter by condition, but that may hide deals
        cond = getenv_str("CONDITION_FILTER", "")
        if cond:
            filters.append(f"conditions:{{{cond}}}")

        # If you want to restrict to North America shipping:
        # filters.append("deliveryCountry:US")

        filter_str = ",".join(filters)

        # Best sort for quick flips is typically ending soon
        sort = "endingSoonest"

        for page in range(page_limit):
            offset = page * max_results_per_query
            params = {
                "q": q,
                "limit": str(max_results_per_query),
                "offset": str(offset),
                "sort": sort,
                "filter": filter_str,
            }

            url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
            r = requests.get(url, headers=base_headers, params=params, timeout=30)
            log(f"BROWSE STATUS: {r.status_code}")
            if r.status_code != 200:
                log(f"BROWSE BODY: {r.text[:400]}")
                break

            data = r.json()
            items = data.get("itemSummaries") or []
            log(f"items returned: {len(items)}")

            if not items:
                break

            for item in items:
                total_seen += 1

                title = title_text(item)
                if not title:
                    continue

                if should_exclude_title(title):
                    continue

                lt = listing_type_from_item(item)
                if lt != "AUCTION":
                    continue

                buy_price, ship_price = extract_prices(item)
                total_price = buy_price + ship_price
                if total_price <= 0:
                    continue
                if total_price > max_total_price:
                    continue

                et = end_time_utc(item)
                if et is None:
                    continue
                if et < end_from or et > end_to:
                    continue

                hrs_left = max((et - now_utc()).total_seconds() / 3600.0, 0.0)

                fb = seller_feedback_percent(item)

                item_id = item.get("itemId") or ""
                item_web_url = item.get("itemWebUrl") or ""

                if not item_id or not item_web_url:
                    continue

                # Estimate sale price
                # Best: marketplace insights median sold
                # Fallback: simple multiplier based on auction inefficiency signals
                est_sale = None
                est_sale = estimate_sale_price_via_marketplace_insights(token, title)

                if est_sale is None:
                    # Fallback estimate: assume market is somewhat higher than current bid for undervalued auctions
                    # Conservative: 1.35x for most, 1.6x if title contains scarcity terms
                    tl = title.lower()
                    mult = 1.35
                    scarcity = ["ssp", "sp", "/10", "/25", "/49", "/50", "/75", "/99", "gold", "refractor", "prizm", "select", "optic", "mosaic", "auto", "autograph", "rookie", "rc"]
                    hit = 0
                    for w in scarcity:
                        if w in tl:
                            hit += 1
                    if hit >= 3:
                        mult = 1.6
                    elif hit == 2:
                        mult = 1.5
                    elif hit == 1:
                        mult = 1.42
                    est_sale = buy_price * mult

                est_profit, roi = compute_profit(buy_price, ship_price, est_sale, out_ship)

                # Keep rules
                keep = False
                if est_profit >= min_profit:
                    keep = True
                elif require_min_profit_floor == 1 and est_profit >= min_profit_floor:
                    # This is your "at least $10 on it" safety net, still respects min buy price
                    keep = True

                if not keep:
                    continue

                score = score_deal(est_profit, roi, hrs_left, fb, total_price)

                # Final cap for anything that slips high
                # If you want a hard cap of 1000 regardless, set MAX_TOTAL_PRICE lower.
                if buy_price > 1000.0:
                    continue

                deal = {
                    "item_id": item_id,
                    "title": title,
                    "listing_url": item_web_url,
                    "sold_comps_url": sold_comps_url_for(title),
                    "listing_type": lt,
                    "buy_price": buy_price,
                    "ship_price": ship_price,
                    "est_sale": est_sale,
                    "est_profit": est_profit,
                    "roi": roi,
                    "score": score,
                    "end_time": et,
                    "seller_feedback_percent": fb,
                }

                total_kept += 1

                if db_enabled:
                    db_upsert_deal(cur, deal)
                    total_inserted += 1

            # Rate limiting friendly pause
            time.sleep(getenv_float("REQUEST_SLEEP_SECONDS", 0.25))

    if db_enabled:
        db_prune_inactive(cur)
        log("db: pruned inactive rows (not seen in latest run)")

    log(f"total_seen: {total_seen}")
    log(f"total_kept: {total_kept}")
    log(f"total_inserted: {total_inserted}")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("SCANNER CRASHED", flush=True)
        print(str(e), flush=True)
        traceback.print_exc()
        raise
