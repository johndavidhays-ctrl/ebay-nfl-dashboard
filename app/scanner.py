# app/scanner.py
import os
import json
import base64
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from .db import SessionLocal, ensure_schema, Item


SCANNER_VERSION = "AUCTIONS_SINGLES_FAST_PROFIT_V1"

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


def browse_search(token: str, query: str, limit: int = 200, offset: int = 0) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    params = {
        "q": query,
        "limit": limit,
        "offset": offset,
        "filter": "buyingOptions:{AUCTION},itemLocationCountry:US",
        "sort": "newlyListed",
    }

    r = requests.get(EBAY_BROWSE_URL, headers=headers, params=params, timeout=30)
    log(f"BROWSE STATUS: {r.status_code}")
    if r.status_code != 200:
        raise RuntimeError(f"eBay browse failed: {r.status_code} {r.text[:500]}")
    return r.json()


def upsert_item(db: Session, query: str, summary: Dict[str, Any]) -> bool:
    ebay_item_id = str(summary.get("itemId") or "").strip()
    if not ebay_item_id:
        return False

    title = str(summary.get("title") or "")[:512]
    url = str(summary.get("itemWebUrl") or "")
    image_url = ""
    img = summary.get("image")
    if isinstance(img, dict):
        image_url = str(img.get("imageUrl") or "")

    price, currency = parse_money(summary.get("price"))
    ship, _ = parse_money(summary.get("shippingOptions", [{}])[0].get("shippingCost") if isinstance(summary.get("shippingOptions"), list) else None)
    total = float(price or 0.0) + float(ship or 0.0)

    condition = str(summary.get("condition") or "")[:128]
    seller = ""
    s = summary.get("seller")
    if isinstance(s, dict):
        seller = str(s.get("username") or "")[:256]

    end_time = parse_end_time(summary)

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
    it.active = True
    it.set_raw(summary)

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

        for q in queries:
            log(f"query: {q}")
            for offset in (0, 200):
                payload = browse_search(token, q, limit=200, offset=offset)
                items = payload.get("itemSummaries") or []
                log(f"items returned: {len(items)}")
                total_seen += len(items)

                for s in items:
                    if not isinstance(s, dict):
                        continue

                    price, _cur = parse_money(s.get("price"))
                    ship, _ = parse_money(s.get("shippingOptions", [{}])[0].get("shippingCost") if isinstance(s.get("shippingOptions"), list) else None)
                    total = float(price or 0.0) + float(ship or 0.0)

                    if total <= 0:
                        continue
                    if total > 1000:
                        continue

                    total_kept += 1
                    if upsert_item(db, q, s):
                        total_inserted += 1

                db.commit()

        pruned = db.query(Item).filter(Item.active.is_(False)).delete(synchronize_session=False)
        db.commit()
        log("db: pruned inactive rows (not seen in latest run)")
        log(f"total_seen: {total_seen}")
        log(f"total_kept: {total_kept}")
        log(f"total_inserted: {total_inserted}")
    finally:
        db.close()


if __name__ == "__main__":
    run()
