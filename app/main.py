import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from app.db import fetch_active_deals, init_db

app = FastAPI()


@app.on_event("startup")
def _startup() -> None:
    # Never crash the whole web app if db is temporarily unavailable
    try:
        init_db()
    except Exception as e:
        print(f"DB init failed: {e}")


@app.get("/")
def health() -> Dict[str, Any]:
    # Always return ok so Render health checks pass
    return {
        "status": "ok",
        "service": "nfl card dashboard",
        "utc": datetime.now(timezone.utc).isoformat(),
    }


def _minutes_away(dt: Optional[datetime]) -> Optional[int]:
    if not dt:
        return None
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    seconds = (dt - now).total_seconds()
    return int(seconds // 60)


@app.get("/deals")
def deals(
    limit: int = Query(200, ge=1, le=1000),
    min_profit: float = Query(0, ge=0),
) -> JSONResponse:
    try:
        data = fetch_active_deals(limit=limit)
        # filter min profit here too, in case scanner wrote negatives
        filtered: List[Dict[str, Any]] = []
        for d in data:
            profit = d.get("profit")
            if profit is None:
                continue
            try:
                if float(profit) < float(min_profit):
                    continue
            except Exception:
                continue

            ends_at = d.get("ends_at")
            d["minutes_away"] = _minutes_away(ends_at)
            filtered.append(d)

        return JSONResponse(filtered)
    except Exception as e:
        # Return useful error instead of Internal Server Error page
        return JSONResponse(
            status_code=500,
            content={"error": "failed_to_fetch_deals", "details": str(e)},
        )
