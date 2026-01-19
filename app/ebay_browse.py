import requests


BROWSE_ENDPOINT = "https://api.ebay.com/buy/browse/v1/item_summary/search"


def browse_search(token: str, query: str) -> dict:
    """
    Broad eBay Browse API search.
    No category restriction.
    No condition restriction.
    No sport restriction.
    """

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    params = {
        "q": query,
        "limit": 50,
        "sort": "price",
    }

    resp = requests.get(
        BROWSE_ENDPOINT,
        headers=headers,
        params=params,
        timeout=30,
    )

    print("BROWSE STATUS:", resp.status_code)
    if resp.status_code != 200:
        print("BROWSE BODY:", resp.text)

    resp.raise_for_status()
    return resp
