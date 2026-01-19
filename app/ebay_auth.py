import base64
import os
import requests

def get_app_token():
    cid = os.environ["EBAY_CLIENT_ID"]
    sec = os.environ["EBAY_CLIENT_SECRET"]

    auth = base64.b64encode(f"{cid}:{sec}".encode()).decode()

    resp = requests.post(
        "https://api.ebay.com/identity/v1/oauth2/token",
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "client_credentials",
            "scope": "https://api.ebay.com/oauth/api_scope/buy.browse",
        },
    )

    print("EBAY OAUTH STATUS:", resp.status_code)
    print("EBAY OAUTH BODY:", resp.text)

    resp.raise_for_status()
    return resp.json()["access_token"]
