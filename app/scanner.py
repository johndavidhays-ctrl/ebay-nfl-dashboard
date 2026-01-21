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


SCANNER_VERSION = "AUCTIONS_SINGLES_MINPROFIT75_V1"

EBAY_BROWSE_SEARCH = "https://api.ebay.com/buy/browse/v1/item_summary/search"
EBAY_BROWSE_ITEM = "https://api.ebay.com/buy/browse/v1/item/"
EBAY_OAUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_SCOPE = "https://api.ebay.com/oauth/api_scope"


def log(msg: str) -> None:
    print(f"SCANNER: {msg}", flush=True)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_access_token() -> str:
    tok = os.getenv("EBAY_OAUTH_TOKEN")
    if tok and tok.strip():
        return tok.strip()

    cid = os.getenv("EBAY_CLIENT_ID")
    sec = os.getenv("EBAY_CLIENT_SECRET")
    if not cid or not sec:
        raise RuntimeError("Missing EBAY_OAUTH_TOKEN or EBAY_CLIENT_ID and EBAY_CLIENT_SECRET")

    basic = base64.b64encode(f"{cid}:{sec}".encode("utf-8")).decode("utf-8")
    r = requests.post(
        EBAY_OAUTH_URL,
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={"grant_type": "client_credentials", "scope": EBAY_SCOPE},
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"eBay token request failed: {r.status_code} {r.text[:300]}")
    data = r.json()
    at = data.get("access_token")
    if not at:
        raise RuntimeError("eBay token response missing access_token")
    return str(at)


def parse_money(m: Optional[Dict[str, Any]]) -> Tuple[float, str]:
    if not m:
        return 0.0, "USD"
    cur = str(m.get("currency") or "USD")
    v = m.get("value")
    try:
        return float(v), cur
    except Exception:
        return 0.0, cur


def shipping_total_from_summary(summary: Dict[str, Any], currency: str) -> float:
    so = summary.get("shippingOptions")
    if isinstance(so, list) and so:
        cost = so[0].get("shippingCost")
        if isinstance(cost, dict):
            v, cur = parse_money(cost)
            if cur == currency:
                return float(v or 0.0)
    return 0.0


def parse_end_time(summary: Dict[str, Any]) -> Optional[datetime]:
    s = summary.get("itemEndDate")
    if not isinstance(s, str) or not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


lot_re = re.compile(r"\b(lot|lots|bundle|bulk|assortment)\b", re.IGNORECASE)
qty_re = re.compile(r"\b(\d+)\s*(cards|card)\b", re.IGNORECASE)


def is_lot_title(title: str) -> bool:
    t = (title or "").strip()
    if not t:
        return True
    if lot_re.search(t):
        return True
    m = qty_re.search(t)
    if m:
        try:
            qty = int(m.group(1))
            if qty >= 2:
                return True
        except Exception:
            pass
    return False


def normalize(text: str) -> str:
    t = (text or "").lower()
    t = re.sub(r"\[[^\]]+\]", " ", t)
    t = re.sub(r"\([^)]*\)", " ", t)
    t = re.sub(r"[^a-z0-9\s/#]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def extract_key_tokens(title: str) -> Dict[str, str]:
    t = normalize(title)

    year = ""
    m = re.search(r"\b(20\d{2})\b", t)
    if m:
        year = m.group(1)

    brands = ["prizm", "mosaic", "optic", "select", "contenders", "donruss", "chrome", "bowman", "topps"]
    brand = ""
    for b in brands:
        if f" {b} " in f" {t} ":
            brand = b
            break

    grade = ""
    for g in ["psa 10", "psa9", "psa 9", "bgs 10", "bgs 9.5", "sgc 10", "sgc 9.5"]:
        if g in t:
            grade = g
            break

    flags = {
        "auto": "auto" if "auto" in t or "autograph" in t else "",
        "numbered": "numbered" if re.search(r"/\d{2,3}\b", t) else "",
        "rookie": "rookie" if "rookie" in t or "rc" in t else "",
    }

    parts = t.split()
    player = ""
    if year:
        try:
            i = parts.index(year)
            nxt = parts[i + 1 : i + 4]
            nxt = [w for w in nxt if w not in brands]
            player = " ".join(nxt[:2]).strip()
        except Exception:
            player = ""

    return {
        "year": year,
        "brand": brand,
        "player": player,
        "grade": grade,
        "auto": flags["auto"],
        "numbered": flags["numbered"],
        "rookie": flags["rookie"],
    }


def build_comp_query(title: str) -> str:
    k = extract_key_tokens(title)
    t = normalize(title)

    keep = []
    if k["year"]:
        keep.append(k["year"])
    if k["brand"]:
        keep.append(k["brand"])
    if k["player"]:
        keep.append(k["player"])
    if k["rookie"]:
        keep.append("rookie")
    if k["auto"]:
        keep.append("auto")
    if k["numbered"]:
        keep.append("numbered")

    if not keep:
        keep = t.split()[:6]

    return " ".join(keep[:8]).strip()


def comp_filter_ok(source_title: str, target_keys: Dict[str, str]) -> bool:
    st = normalize(source_title)

    if target_keys["year"] and target_keys["year"] not in st:
        return False
    if target_keys["brand"] and target_keys["brand"] not in st:
        return False

    if target_keys["player"]:
        last = target_keys["player"].split()[-1]
        if last and last not in st:
            return False

    if target_keys["grade"]:
        if "psa" in target_keys["grade"] and "psa" not in st:
            return False
        if "bgs" in target_keys["grade"] and "bgs" not in st:
            return False
        if "sgc" in target_keys["grade"] and "sgc" not in st:
            return False

    if target_keys["auto"]:
        if ("auto" not in st) and ("autograph" not in st):
            return False

    if target_keys["rookie"]:
        if ("rookie" not in st) and (" rc " not in f" {st} "):
            return False

    return True


def browse_search(token: str, q: str, buying: str, limit: int = 200, offset: int = 0, sort: str = "endingSoonest") -> Dict[str, Any]:
    r = requests.get(
        EBAY_BROWSE_SEARCH,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        params={
            "q": q,
            "limit": limit,
            "offset": offset,
            "filter": f"buyingOptions:{{{buying}}},itemLocationCountry:US",
            "sort": sort,
        },
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Browse search failed: {r.status_code} {r.text[:300]}")
    return r.json()


def estimate_market_value_conservative(token: str, title: str, currency: str) -> Tuple[float, int]:
    keys = extract_key_tokens(title)
    comp_q = build_comp_query(title)

    payload = browse_search(token, comp_q, buying="FIXED_PRICE", limit=80, offset=0, sort="price")
    items = payload.get("itemSummaries") or []

    totals: List[float] = []
    for s in items:
        if not isinstance(s, dict):
            continue
        stitle = str(s.get("title") or "")
        if not comp_filter_ok(stitle, keys):
            continue

        p, cur = parse_money(s.get("price"))
        if cur != currency or p <= 0:
            continue
        ship = shipping_total_from_summary(s, currency)
        tot = float(p) + float(ship)
        if tot <= 0:
            continue
        totals.append(tot)

    if len(totals) < 10:
        return 0.0, len(totals)

    totals.sort()
    idx = int(len(totals) * 0.25)
    idx = max(0, min(idx, len(totals) - 1))
    return float(totals[idx]), len(totals)


def compute_profit(market: float, total_buy: float) -> float:
    fee_rate = float(os.getenv("FEE_RATE", "0.145"))
    sell_ship_buffer = float(os.getenv("SELL_SHIP_BUFFER", "6.50"))
    promo_rate = float(os.getenv("PROMO_RATE", "0.00"))

    fees = market * fee_rate
    promo = market * promo_rate
    return round(float(market) - float(total_buy) - float(fees) - float(promo) - float(sell_ship_buffer), 2)


def upsert(db: Session, summary: Dict[str, Any], query: str, currency: str, total_buy: float, market: float, profit: float) -> None:
    item_id = str(summary.get("itemId") or "").strip()
    if not item_id:
        return

    title = str(summary.get("title") or "")[:512]
    url = str(summary.get("itemWebUrl") or "")
    image_url = ""
    img = summary.get("image")
    if isinstance(img, dict):
        image_url = str(img.get("imageUrl") or "")

    end_time = parse_end_time(summary)

    existing = db.scalar(select(Item).where(Item.ebay_item_id == item_id))
    it = existing if existing else Item(ebay_item_id=item_id)

    it.title = title
    it.url = url
    it.image_url = image_url
    it.query = query
    it.currency = currency
    it.price = float(summary.get("price", {}).get("value", 0) or 0)
    it.shipping = float(total_buy - float(it.price or 0))
    it.total_price = float(total_buy)
    it.end_time = end_time
    it.market_value = float(market)
    it.profit = float(profit)
    it.active = True

    try:
        it.raw_json = json.dumps(summary, ensure_ascii=False, default=str)
    except Exception:
        it.raw_json = "{}"

    db.add(it)


def run() -> None:
    log(f"SCANNER VERSION: {SCANNER_VERSION}")
    ensure_schema()

    min_profit = float(os.getenv("MIN_PROFIT", "75"))
    token = get_access_token()

    queries = [
        "football psa 10",
        "prizm psa 10",
        "mosaic psa 10",
        "optic psa 10",
        "rookie auto",
        "on card auto",
        "numbered /99",
        "ssp football",
        "case hit football",
    ]

    db = SessionLocal()
    try:
        db.query(Item).update({Item.active: False})
        db.commit()

        seen = 0
        inserted = 0
        kept = 0

        for q in queries:
            payload = browse_search(token, q, buying="AUCTION", limit=200, offset=0, sort="endingSoonest")
            items = payload.get("itemSummaries") or []

            for s in items:
                if not isinstance(s, dict):
                    continue
                seen += 1

                title = str(s.get("title") or "")
                if is_lot_title(title):
                    continue

                p, cur = parse_money(s.get("price"))
                if cur != "USD":
                    continue

                ship = shipping_total_from_summary(s, cur)
                total = float(p) + float(ship)
                if total <= 0:
                    continue
                if total > 2000:
                    continue

                end_time = parse_end_time(s)
                if not end_time:
                    continue

                market, comps_used = estimate_market_value_conservative(token, title, cur)
                if market <= 0:
                    continue

                profit = compute_profit(market, total)
                if profit < min_profit:
                    continue

                kept += 1
                upsert(db, s, q, cur, total, market, profit)
                inserted += 1

            db.commit()

        db.query(Item).filter(Item.active.is_(False)).delete(synchronize_session=False)
        db.commit()

        log(f"seen: {seen}")
        log(f"kept: {kept}")
        log(f"inserted: {inserted}")

    finally:
        db.close()


if __name__ == "__main__":
    run()
