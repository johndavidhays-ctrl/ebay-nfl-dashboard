import os
from typing import Any, Dict, List

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def _get_database_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is missing")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def get_engine() -> Engine:
    return create_engine(
        _get_database_url(),
        pool_pre_ping=True,
        future=True,
    )


def init_db() -> None:
    engine = get_engine()
    ddl = """
    CREATE TABLE IF NOT EXISTS deals (
        id BIGSERIAL PRIMARY KEY,
        item_id TEXT UNIQUE NOT NULL,
        title TEXT NOT NULL,
        url TEXT NOT NULL,
        image_url TEXT,
        query TEXT,
        total_cost NUMERIC,
        market NUMERIC,
        profit NUMERIC,
        ends_at TIMESTAMPTZ,
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def fetch_active_deals(limit: int = 200) -> List[Dict[str, Any]]:
    engine = get_engine()
    sql = """
    SELECT
        id,
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
    with engine.begin() as conn:
        rows = conn.execute(text(sql), {"limit": int(limit)}).mappings().all()
        return [dict(r) for r in rows]
