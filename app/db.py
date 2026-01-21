import os
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL")


def get_engine():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def init_db():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS deals (
                    id BIGSERIAL PRIMARY KEY,
                    item_id TEXT UNIQUE,
                    title TEXT,
                    url TEXT,
                    image_url TEXT,
                    query TEXT,
                    total_cost NUMERIC,
                    market NUMERIC,
                    profit NUMERIC,
                    end_time TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    is_active BOOLEAN DEFAULT TRUE
                )
                """
            )
        )
    return engine


def upsert_deal(engine, deal):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO deals (
                    item_id, title, url, image_url, query,
                    total_cost, market, profit, end_time,
                    updated_at, is_active
                )
                VALUES (
                    :item_id, :title, :url, :image_url, :query,
                    :total_cost, :market, :profit, :end_time,
                    :updated_at, true
                )
                ON CONFLICT (item_id)
                DO UPDATE SET
                    title = EXCLUDED.title,
                    url = EXCLUDED.url,
                    image_url = EXCLUDED.image_url,
                    query = EXCLUDED.query,
                    total_cost = EXCLUDED.total_cost,
                    market = EXCLUDED.market,
                    profit = EXCLUDED.profit,
                    end_time = EXCLUDED.end_time,
                    updated_at = EXCLUDED.updated_at,
                    is_active = true
                """
            ),
            {
                **deal,
                "updated_at": datetime.now(timezone.utc),
            },
        )


def fetch_active_deals(engine):
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
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
                    end_time
                FROM deals
                WHERE is_active = true
                ORDER BY end_time ASC
                """
            )
        ).mappings().all()
        return list(rows)
