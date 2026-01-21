# app/scanner.py
import os
import re
import json
import base64
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from .db import SessionLocal, ensure_schema, Item


SCANNER_VERSION = "AUCTIONS_SINGLES_FAST_PROFIT_V2"

EBAY_BROWSE_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
EBAY_OAUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_SCOPE = "https://api.ebay.com/oauth/api_scope"


def log(msg: str) -> None:
    print(f"SCANNER: {msg}", flush=True)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_access_token() -> str:
    token = os.getenv("EBAY_OAUTH_TOKEN")
    if token and token.strip():
        return token.strip()

    client_id = os.getenv("EBAY_CLIENT_ID")
    client_secret = os.getenv("EBAY_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("Missing EBAY_OAUTH_TOKEN or EBAY_CLIENT_ID and EBAY_CLIENT_SECRET")

    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")
    headers = {
        "Authorization": f"Basic {basic}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials",
        "scope": EBAY_SCOPE,
    }

    r = requests.post(EBAY_OAUTH_URL, headers=headers, data=data, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"eBay token request failed: {r.status_code} {r.text[:500]}")
    payload = r.json()
    access_token = payload.get("access_token")
    if not access_token:
        raise RuntimeError("eBay token response missing access_token")
    return str(access_token)


def parse_money(m: Optional[Dict[str, Any]]) -> Tuple[float, str]:
    if not m:
        return 0.0, "USD"
    cur = m.get("currency") or "USD"
    v = m.get("value")
    try:
        return float(v), str(cur)
    except Exception:
        return 0.0, str(cur)


def parse_end_time(item: Dict[str, Any]) -> Optional[datetime]:
    end = None
    if isinstance(item.get("itemEndDate"), str):
        end = item["itemEndDate"]
    if not end:
        return None
    try:
        return datetime.fromisoformat(end.replace("Z", "+00:00"))
    except Exception:
        return None


def browse_search(token: str, query: str, limit: int = 200, offset: int = 0, buying: str = "AUCTION", sort: str = "newlyListed") -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    params = {
        "q": query,
        "limit": limit,
        "offset": offset,
        "filter": f"buyingOptions:{{{buying}}},itemLocationCountry:US",
        "sort": sort,
    }

    r = requests.get(EBAY_BROWSE_URL, headers=headers, params=params, timeout=30)
    log(f"BROWSE STATUS: {r.status_code}")
    if r.status_code != 200:
        raise RuntimeError(f"eBay browse failed: {r.status_code} {r.text[:500]}")
    return r.json()


_lot_re = re.compile(r"\b(lot|lots|bundle|bulk|assortment)\b", re.IGNORECASE)
_qty_re = re.compile(r"\b(\d+)\s*(cards|card)\b", re.IGNORECASE)


def is_lot_title(title: str) -> bool:
    t = (title or "").strip()
    if not t:
        return True
    if _lot_re.search(t):
        return True
    m = _qty_re.search(t)
    if m:
        try:
            qty = int(m.group(1))
            if qty >= 2:
                return True
        except Exception:
            return False
    return False


def _safe_ship(summary: Dict[str, Any]) -> float:
    so = summary.get("shippingOptions")
    if isinstance(so, list) and so:
        cost = so[0].get("shippingCost")
        v, _ = parse_money(cost if isinstance(cost, dict) else None)
        return float(v or 0.0)
    return 0.0


def _title_to_comp_query(title: str) -> str:
    t = (title or "").lower()
    t = re.sub(r"\[[^\]]+\]", " ", t)
    t = re.sub(r"\([^)]*\)", " ", t)
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    words = [w for w in t.split(" ") if w and w not in {"the", "and", "with", "for"}]
    if len(words) > 8:
        words = words[:8]
    return " ".join(words)


def estimate_market_value(token: str, title: str, currency: str) -> float:
    comp_q = _title_to_comp_query(title)
    if not comp_q:
        return 0.0

    payload = browse_search(token, comp_q, limit=50, offset=0, buying="FIXED_PRICE", sort="price")
    items = payload.get("itemSummaries") or []
    totals: List[float] = []
    for s in items:
        if not isinstance(s, dict):
            continue
        p, cur = parse_money(s.get("price"))
        if cur != currency:
            continue
        ship = _safe_ship(s)
        total = float(p or 0.0) + float(ship or 0.0)
        if total <= 0:
            continue
        totals.append(total)

    if not totals:
        return 0.0

    totals.sort()
    n = len(totals)
    mid = n // 2
    if n % 2 == 1:
        return float(totals[mid])
    return float((totals[mid - 1] + totals[mid]) / 2.0)


def compute_profit(market_value: float, total_price: float) -> float:
    fee_rate = float(os.getenv("EBAY_FEE_RATE", "0.13"))
    shipping_buffer = float(os.getenv("SHIPPING_BUFFER", "0.00"))
    return float(market_value) - float(total_price) - (float(market_value) * fee_rate) - float(shipping_buffer)


def upsert_item(
    db: Session,
    token: str,
    query: str,
    summary: Dict[str, Any],
    comp_cache: Dict[str, float],
) -> bool:
    ebay_item_id = str(summary.get("itemId") or "").strip()
    if not ebay_item_id:
        return False

    title = str(summary.get("title") or "")[:512]
    if is_lot_title(title):
        return False

    url = str(summary.get("itemWebUrl") or "")
    image_url = ""
    img = summary.get("image")
    if isinstance(img, dict):
        image_url = str(img.get("imageUrl") or "")

    price, currency = parse_money(summary.get("price"))
    ship = _safe_ship(summary)
    total = float(price or 0.0) + float(ship or 0.0)

    condition = str(summary.get("condition") or "")[:128]
    seller = ""
    s = summary.get("seller")
    if isinstance(s, dict):
        seller = str(s.get("username") or "")[:256]

    end_time = parse_end_time(summary)

    comp_key = _title_to_comp_query(title)
    if comp_key in comp_cache:
        market_value = comp_cache[comp_key]
    else:
        market_value = estimate_market_value(token, title, currency)
        comp_cache[comp_key] = market_value

    profit = compute_profit(market_value, total)

    existing = db.scalar(select(Item).where(Item.ebay_item_id == ebay_item_id))
    if existing:
        it = existing
    else:
        it = Item(ebay_item_id=ebay_item_id)

    it.title = title
    it.url = url
    it.image_url = image_url
    it.query = query
    it.currency = currency
    it.price = float(price or 0.0)
    it.shipping = float(ship or 0.0)
    it.total_price = float(total or 0.0)
    it.end_time = end_time
    it.condition = condition
    it.seller = seller
    it.market_value = float(market_value or 0.0)
    it.profit = float(profit or 0.0)
    it.active = True

    try:
        it.raw_json = json.dumps(summary, ensure_ascii=False, default=str)
    except Exception:
        it.raw_json = "{}"

    db.add(it)
    return True


def run() -> None:
    log(f"SCANNER VERSION: {SCANNER_VERSION}")
    ensure_schema()

    token = get_access_token()

    queries = [
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

    db = SessionLocal()
    try:
        log("db: connected, schema ensured, marked all inactive")
        db.query(Item).update({Item.active: False})
        db.commit()

        total_seen = 0
        total_kept = 0
        total_inserted = 0

        log(f"queries: {len(queries)}")
        comp_cache: Dict[str, float] = {}

        for q in queries:
            log(f"query: {q}")
            for offset in (0, 200):
                payload = browse_search(token, q, limit=200, offset=offset, buying="AUCTION", sort="newlyListed")
                items = payload.get("itemSummaries") or []
                log(f"items returned: {len(items)}")
                total_seen += len(items)

                for s in items:
                    if not isinstance(s, dict):
                        continue

                    title = str(s.get("title") or "")
                    if is_lot_title(title):
                        continue

                    price, _cur = parse_money(s.get("price"))
                    ship = _safe_ship(s)
                    total = float(price or 0.0) + float(ship or 0.0)

                    if total <= 0:
                        continue
                    if total > 1000:
                        continue

                    total_kept += 1
                    if upsert_item(db, token, q, s, comp_cache):
                        total_inserted += 1

                db.commit()

        db.query(Item).filter(Item.active.is_(False)).delete(synchronize_session=False)
        db.commit()

        log("db: pruned inactive rows (not seen in latest run)")
        log(f"total_seen: {total_seen}")
        log(f"total_kept: {total_kept}")
        log(f"total_inserted: {total_inserted}")
    finally:
        db.close()


if __name__ == "__main__":
    run()
