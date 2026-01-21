import os
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


_engine: Engine | None = None


def _get_engine() -> Engine:
    global _engine
    if _engine is not None:
        return _engine

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is missing")

    # Render provides postgres URLs that work fine as is.
    _engine = create_engine(db_url, pool_pre_ping=True)
    return _engine


def init_db() -> None:
    """
    Creates the deals table if missing, and adds any missing columns.
    IMPORTANT: No 'id' column. item_id is the primary key.
    """
    eng = _get_engine()
    with eng.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS deals (
                    item_id TEXT PRIMARY KEY,
                    title TEXT,
                    url TEXT,
                    image_url TEXT,
                    query TEXT,
                    total_cost NUMERIC,
                    market NUMERIC,
                    profit NUMERIC,
                    ends_at TIMESTAMPTZ,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
        )

        # If the table already existed from older versions, ensure columns exist.
        # These are safe no ops if they already exist.
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS title TEXT;"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS url TEXT;"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS image_url TEXT;"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS query TEXT;"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS total_cost NUMERIC;"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS market NUMERIC;"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS profit NUMERIC;"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS ends_at TIMESTAMPTZ;"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();"))


def fetch_active_deals(limit: int = 200) -> list[dict[str, Any]]:
    """
    Returns active deals sorted by ends_at soonest.
    Also includes minutes_away for the UI.
    """
    eng = _get_engine()
    sql = text(
        """
        SELECT
            item_id,
            title,
            url,
            image_url,
            query,
            total_cost,
            market,
            profit,
            ends_at,
            is_active,
            created_at,
            updated_at
        FROM deals
        WHERE is_active = TRUE
        ORDER BY ends_at ASC NULLS LAST
        LIMIT :limit;
        """
    )

    now = datetime.now(timezone.utc)

    with eng.begin() as conn:
        rows = conn.execute(sql, {"limit": int(limit)}).mappings().all()

    out: list[dict[str, Any]] = []
    for r in rows:
        ends_at = r.get("ends_at")
        minutes_away = None
        if ends_at is not None:
            try:
                delta = ends_at - now
                minutes_away = int(delta.total_seconds() // 60)
                if minutes_away < 0:
                    minutes_away = 0
            except Exception:
                minutes_away = None

        out.append(
            {
                "item_id": r.get("item_id"),
                "title": r.get("title"),
                "url": r.get("url"),
                "image_url": r.get("image_url"),
                "query": r.get("query"),
                "total_cost": float(r.get("total_cost") or 0),
                "market": float(r.get("market") or 0),
                "profit": float(r.get("profit") or 0),
                "ends_at": r.get("ends_at").isoformat() if r.get("ends_at") else None,
                "minutes_away": minutes_away,
            }
        )

    return out
