# app/db.py

import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor


def _database_url() -> str:
    url = (os.environ.get("DATABASE_URL") or "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
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
    Safe to run every time.
    Creates the table if missing and adds any columns we rely on.
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
                ADD COLUMN IF NOT EXISTS listing_type TEXT,
                ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'new',
                ADD COLUMN IF NOT EXISTS fb_pct NUMERIC,
                ADD COLUMN IF NOT EXISTS fb_score NUMERIC;
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS deals_score_idx
                ON deals (score DESC);
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS deals_created_at_idx
                ON deals (created_at DESC);
                """
            )

        conn.commit()


def fetch_deals(limit: int = 200):
    """
    Used by the dashboard.
    Returns a list of dict rows, best first.
    """
    init_db()

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    item_id,
                    title,
                    item_url,
                    sold_url,
                    buy_price,
                    buy_shipping,
                    est_profit,
                    roi,
                    score,
                    listing_type,
                    status,
                    fb_pct,
                    fb_score,
                    created_at
                FROM deals
                ORDER BY
                    score DESC NULLS LAST,
                    est_profit DESC NULLS LAST,
                    created_at DESC
                LIMIT %s;
                """,
                (limit,),
            )
            rows = cur.fetchall()
            return rows


def update_status(item_id: str, status: str) -> None:
    """
    Used by the dashboard action buttons.
    Typical statuses: new, watching, bought, passed, sold
    """
    if not item_id:
        return

    init_db()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE deals
                SET status = %s
                WHERE item_id = %s;
                """,
                (status, item_id),
            )
        conn.commit()
