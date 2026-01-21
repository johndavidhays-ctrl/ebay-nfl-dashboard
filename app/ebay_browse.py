import os
from typing import Any, Dict, List, Optional

import requests

from app.ebay_auth import get_app_token


BROWSE_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"


def _headers() -> Dict[str, str]:
    token = get_app_token()
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }


def search_browse(
    query: str,
    buying_option: str,
    limit: int = 200,
    sort: str = "endingSoonest",
    category_ids: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    buying_option: AUCTION or FIXED_PRICE
    """
    params: Dict[str, Any] = {
        "q": query,
        "limit": limit,
        "sort": sort,
        "filter": f"buyingOptions:{{{buying_option}}}",
    }

    if category_ids:
        params["category_ids"] = category_ids

    resp = requests.get(BROWSE_SEARCH_URL, headers=_headers(), params=params, timeout=25)
    if resp.status_code == 401:
        raise RuntimeError(f"401 Unauthorized from Browse API. Token invalid. Body: {resp.text[:200]}")
    resp.raise_for_status()
    data = resp.json()
    return data.get("itemSummaries", []) or []


def usd_amount(price_obj: Optional[Dict[str, Any]]) -> float:
    if not price_obj:
        return 0.0
    try:
        return float(price_obj.get("value", 0.0))
    except Exception:
        return 0.0


def item_total_cost(item: Dict[str, Any]) -> float:
    price = usd_amount(item.get("price"))
    shipping = usd_amount(item.get("shippingOptions", [{}])[0].get("shippingCost")) if item.get("shippingOptions") else 0.0
    return price + shipping


def item_end_time(item: Dict[str, Any]) -> Optional[str]:
    # endDate is usually inside item.get("itemEndDate") or inside buyingOptions.
    end = item.get("itemEndDate")
    if end:
        return end
    # fallback
    opts = item.get("buyingOptions") or []
    _ = opts
    return None
