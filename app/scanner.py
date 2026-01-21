import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.db import init_db, mark_all_inactive, prune_inactive, upsert_deal
from app.ebay_browse import item_total_cost, search_browse, usd_amount


SCANNER_VERSION = "AUCTIONS_SINGLES_MINPROFIT150_OAUTHFIX_V1"

DEFAULT_MIN_PROFIT = float(os.getenv("MIN_PROFIT", "150").strip() or "150")
DEFAULT_FEE_RATE = float(os.getenv("FEE_RATE", "0.13").strip() or "0.13")
DEFAULT_MARKET_HAIRCURT = float(os.getenv("MARKET_HAIRCUT", "0.90").strip() or "0.90")

CATEGORY_SPORTS_CARDS = os.getenv("EBAY_CATEGORY_IDS", "").strip()  # optional


LOT_BAD_WORDS = [
    "lot",
    "lots",
    "bundle",
    "bundles",
    "bulk",
    "pack",
    "packs",
    "break",
    "breaks",
    "case",
    "cases",
    "team lot",
    "player lot",
    "mystery",
]


def _log(msg: str):
    print(f"SCANNER: {msg}", flush=True)


def _utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_end_time(item: Dict[str, Any]) -> Optional[datetime]:
    s = item.get("itemEndDate") or item.get("endDate") or None
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _is_lot(title: str) -> bool:
    t = (title or "").lower()
    for w in LOT_BAD_WORDS:
        if w in t:
            return True
    return False


def _looks_like_single_card(title: str) -> bool:
    """
    Best effort single card filter.
    Reject obvious lots, breaks, packs.
    Keep graded singles and individual card keywords.
    """
    if _is_lot(title):
        return False

    t = (title or "").lower()

    if "cards" in t and "1 card" not in t:
        return False

    if "random" in t:
        return False

    return True


def _normalize_for_market(title: str) -> str:
    t = (title or "")
    t = re.sub(r"\b(auction|ending|ends|no reserve|nr)\b", " ", t, flags=re.I)
    t = re.sub(r"\b(lot|bundle|break|packs?)\b", " ", t, flags=re.I)
    t = re.sub(r"\s+", " ", t).strip()
    return t[:120]


def _estimate_market_fixed_price(title: str) -> float:
    """
    Uses fixed price listings as a proxy.
    Conservative: take the lower quartile-ish by sorting and sampling low priced results.
    """
    q = _normalize_for_market(title)
    if not q:
        return 0.0

    items = search_browse(q, buying_option="FIXED_PRICE", limit=50, sort="price")
    costs = []
    for it in items:
        costs.append(item_total_cost(it))
    costs = [c for c in costs if c > 0]
    if not costs:
        return 0.0

    costs.sort()
    idx = max(0, int(len(costs) * 0.25) - 1)
    return float(costs[idx])


def _profit_estimate(market: float, total_cost: float) -> float:
    """
    Conservative profit:
    take market haircut then subtract fees then subtract total cost
    """
    net_sale = market * DEFAULT_MARKET_HAIRCURT
    fees = net_sale * DEFAULT_FEE_RATE
    return net_sale - fees - total_cost


def _queries() -> List[str]:
    base = [
        "psa 10 football",
        "psa 10 rookie",
        "prizm rookie psa 10",
        "downtown psa 10",
        "kaboom psa 10",
        "color blast psa 10",
        "gold /10 psa 10",
        "black 1/1 psa",
        "on card auto psa 10",
        "ssp psa 10 football",
        "case hit psa 10",
    ]
    # you can override by env QUERIES separated by |
    raw = os.getenv("QUERIES", "").strip()
    if raw:
        parts = [p.strip() for p in raw.split("|") if p.strip()]
        if parts:
            return parts
    return base


def run():
    _log(f"SCANNER VERSION: {SCANNER_VERSION}")

    eng = init_db()
    with Session(eng) as session:
        _log("db: connected, schema ensured, marked all inactive")
        mark_all_inactive(session)

        queries = _queries()
        _log(f"queries: {len(queries)}")

        seen = 0
        kept = 0

        for q in queries:
            _log(f"query: {q}")
            items = search_browse(q, buying_option="AUCTION", limit=200, sort="endingSoonest", category_ids=CATEGORY_SPORTS_CARDS)
            _log(f"items returned: {len(items)}")

            for it in items:
                seen += 1

                title = it.get("title") or ""
                if not title:
                    continue

                if not _looks_like_single_card(title):
                    continue

                total_cost = item_total_cost(it)
                if total_cost <= 0:
                    continue

                market = _estimate_market_fixed_price(title)
                if market <= 0:
                    continue

                profit = _profit_estimate(market, total_cost)

                if profit < DEFAULT_MIN_PROFIT:
                    continue

                item_id = it.get("itemId") or ""
                url = it.get("itemWebUrl") or ""
                img = ""
                if it.get("image") and isinstance(it["image"], dict):
                    img = it["image"].get("imageUrl", "") or ""

                end_time = _parse_end_time(it)

                if not item_id or not url:
                    continue

                upsert_deal(
                    session,
                    {
                        "item_id": item_id,
                        "title": title,
                        "url": url,
                        "image_url": img,
                        "query": q,
                        "total_cost": float(total_cost),
                        "market": float(market),
                        "profit": float(profit),
                        "end_time": end_time,
                    },
                )
                kept += 1

        _log("db: pruned inactive rows (not seen in latest run)")
        prune_inactive(session)

        _log(f"seen: {seen}")
        _log(f"kept: {kept}")
        _log(f"min_profit: {DEFAULT_MIN_PROFIT}")
        _log(f"fee_rate: {DEFAULT_FEE_RATE}")
        _log(f"market_haircut: {DEFAULT_MARKET_HAIRCURT}")


if __name__ == "__main__":
    run()
