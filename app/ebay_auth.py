import base64
import os
import time
from dataclasses import dataclass
from typing import Optional

import requests


EBAY_OAUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_SCOPE = "https://api.ebay.com/oauth/api_scope"


@dataclass
class Token:
    access_token: str
    expires_at_epoch: int


_cached: Optional[Token] = None


def _now() -> int:
    return int(time.time())


def get_app_token() -> str:
    """
    Gets an application OAuth token using client credentials.
    Caches in memory until near expiry.
    """
    global _cached

    if _cached and _cached.expires_at_epoch - _now() > 60:
        return _cached.access_token

    client_id = os.getenv("EBAY_CLIENT_ID", "").strip()
    client_secret = os.getenv("EBAY_CLIENT_SECRET", "").strip()

    if not client_id or not client_secret:
        raise RuntimeError("Missing EBAY_CLIENT_ID or EBAY_CLIENT_SECRET in environment")

    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")

    headers = {
        "Authorization": f"Basic {basic}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {
        "grant_type": "client_credentials",
        "scope": EBAY_SCOPE,
    }

    last_err = None
    for _ in range(3):
        try:
            resp = requests.post(EBAY_OAUTH_URL, headers=headers, data=data, timeout=20)
            if resp.status_code >= 400:
                raise RuntimeError(f"eBay OAuth error {resp.status_code}: {resp.text[:300]}")
            payload = resp.json()
            token = payload["access_token"]
            expires_in = int(payload.get("expires_in", 7200))
            _cached = Token(access_token=token, expires_at_epoch=_now() + expires_in)
            return token
        except Exception as e:
            last_err = e
            time.sleep(1)

    raise RuntimeError(f"Failed to get eBay OAuth token: {last_err}")
