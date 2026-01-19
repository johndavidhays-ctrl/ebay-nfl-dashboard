import base64
import os
import requests

def get_app_token():
    cid = os.environ["EBAY_CLIENT_ID"]
    sec = os.environ["EBAY_CLIENT_SECRET"]

    token = requests.post(
        "https://api.ebay.com/identity/v1/oauth2/token",
        headers={
            "Authorization": "Basic " + base64.b64encode(f"{cid}:{sec}".encode()).decode(),
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "client_credentials",
            "scope": "https://api.ebay.com/oauth/api_scope"
        }
    ).json()["access_token"]

    return token
