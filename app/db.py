import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS deals (
                    item_id TEXT PRIMARY KEY,
                    title TEXT,
                    item_url TEXT,
                    sold_url TEXT,
                    buy_price NUMERIC,
                    buy_shipping NUMERIC,
                    est_profit NUMERIC,
                    roi NUMERIC,
                    score NUMERIC,
                    listing_type TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)

            cur.execute("""
                ALTER TABLE deals
                ADD COLUMN IF NOT EXISTS est_profit NUMERIC,
                ADD COLUMN IF NOT EXISTS roi NUMERIC,
                ADD COLUMN IF NOT EXISTS score NUMERIC,
                ADD COLUMN IF NOT EXISTS listing_type TEXT,
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()
            """)
        conn.commit()


def fetch_deals(limit=250):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    item_id,
                    title,
                    item_url,
                    sold_url,
                    COALESCE(listing_type,'') AS listing_type,
                    COALESCE(buy_price,0) AS buy_price,
                    COALESCE(buy_shipping,0) AS buy_shipping,
                    COALESCE(est_profit,0) AS est_profit,
                    COALESCE(roi,0) AS roi,
                    COALESCE(score,0) AS score
                FROM deals
                WHERE COALESCE(est_profit,0) >= 20
                ORDER BY score DESC, est_profit DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
