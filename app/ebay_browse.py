import requests

def browse_search(token, q):
    return requests.get(
        "https://api.ebay.com/buy/browse/v1/item_summary/search",
        headers={
            "Authorization": f"Bearer {token}",
            "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
        },
        params={
            "q": q,
            "category_ids": "212",
            "limit": 50,
            "sort": "newlyListed",
        },
    ).json()
