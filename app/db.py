# app/db.py

import os
import psycopg2
from contextlib import contextmanager


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is not set in the environment")
    return url


@contextmanager
def get_conn():
    conn = psycopg2.connect(_database_url(), sslmode="require")
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def init_db() -> None:
    """
    Ensures the deals table exists and ensures the scoring columns exist.
    Safe to run every time the scanner starts.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS deals (
                    item_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    item_url TEXT NOT NULL,
                    sold_url TEXT NOT NULL,
                    buy_price NUMERIC NOT NULL,
                    buy_shipping NUMERIC NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

            cur.execute(
                """
                ALTER TABLE deals
                ADD COLUMN IF NOT EXISTS est_profit NUMERIC,
                ADD COLUMN IF NOT EXISTS roi NUMERIC,
                ADD COLUMN IF NOT EXISTS score NUMERIC,
                ADD COLUMN IF NOT EXISTS listing_type TEXT;
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS deals_score_idx
                ON deals (score DESC);
                """
            )

        conn.commit()
