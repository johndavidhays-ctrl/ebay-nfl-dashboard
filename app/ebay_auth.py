import base64
import os
import requests


def get_app_token() -> str:
    """
    Gets an eBay application access token using client credentials.
    Uses the broad scope that should work with most new apps.
    """
    client_id = os.environ["EBAY_CLIENT_ID"].strip()
    client_secret = os.environ["EBAY_CLIENT_SECRET"].strip()

    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")

    resp = requests.post(
        "https://api.ebay.com/identity/v1/oauth2/token",
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "client_credentials",
            "scope": "https://api.ebay.com/oauth/api_scope",
        },
        timeout=30,
    )

    print("EBAY OAUTH STATUS:", resp.status_code)
    print("EBAY OAUTH BODY:", resp.text)

    resp.raise_for_status()

    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in response: {data}")

    return token
