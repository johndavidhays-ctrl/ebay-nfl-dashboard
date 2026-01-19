import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL)

def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS deals (
                id SERIAL PRIMARY KEY,
                item_id TEXT UNIQUE,
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
            conn.commit()

def fetch_deals(limit=200):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM deals
                ORDER BY score DESC NULLS LAST,
                         est_profit DESC NULLS LAST
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
