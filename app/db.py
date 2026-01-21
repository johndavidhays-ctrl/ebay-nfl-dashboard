import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")


def get_conn():
    return psycopg2.connect(DATABASE_URL)


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
                created_at TIMESTAMP DEFAULT NOW(),
                last_seen_at TIMESTAMP DEFAULT NOW(),
                active BOOLEAN DEFAULT TRUE
            );
            """)
            conn.commit()


def fetch_deals(limit=200):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM deals
                WHERE active = TRUE
                ORDER BY score DESC, est_profit DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
