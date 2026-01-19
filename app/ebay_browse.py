# app/ebay_browse.py

import requests

BROWSE_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

def browse_search(token: str, query: str, *, limit: int = 50, offset: int = 0, include_auctions: bool = True):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    buying = "AUCTION|FIXED_PRICE" if include_auctions else "FIXED_PRICE"

    params = {
        "q": query,
        "limit": str(limit),
        "offset": str(offset),
        "filter": f"buyingOptions:{{{buying}}}",
    }

    resp = requests.get(BROWSE_SEARCH_URL, headers=headers, params=params, timeout=30)
    print("BROWSE STATUS:", resp.status_code)

    resp.raise_for_status()
    return resp.json()
