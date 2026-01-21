import os
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

    _engine = create_engine(db_url, pool_pre_ping=True)
    return _engine


def init_db() -> None:
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


def mark_all_inactive() -> None:
    eng = _get_engine()
    with eng.begin() as conn:
        conn.execute(text("UPDATE deals SET is_active = FALSE;"))


def prune_inactive() -> None:
    eng = _get_engine()
    with eng.begin() as conn:
        conn.execute(text("DELETE FROM deals WHERE is_active = FALSE;"))


def upsert_deal(deal: dict[str, Any]) -> None:
    eng = _get_engine()
    with eng.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO deals (
                    item_id, title, url, image_url, query,
                    total_cost, market, profit, ends_at,
                    is_active, created_at, updated_at
                )
                VALUES (
                    :item_id, :title, :url, :image_url, :query,
                    :total_cost, :market, :profit, :ends_at,
                    TRUE, NOW(), NOW()
                )
                ON CONFLICT (item_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    url = EXCLUDED.url,
                    image_url = EXCLUDED.image_url,
                    query = EXCLUDED.query,
                    total_cost = EXCLUDED.total_cost,
                    market = EXCLUDED.market,
                    profit = EXCLUDED.profit,
                    ends_at = EXCLUDED.ends_at,
                    is_active = TRUE,
                    updated_at = NOW();
                """
            ),
            deal,
        )
