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
                item_id TEXT PRIMARY KEY,
                title TEXT,
                item_url TEXT,
                sold_url TEXT,
                buy_price NUMERIC,
                buy_shipping NUMERIC
            );
            """)

            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS est_profit NUMERIC;")
            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS roi NUMERIC;")
            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS score NUMERIC;")
            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS listing_type TEXT;")

            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS created_at TIMESTAMP;")
            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP;")
            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS active BOOLEAN;")

            cur.execute("UPDATE deals SET created_at = NOW() WHERE created_at IS NULL;")
            cur.execute("UPDATE deals SET last_seen_at = NOW() WHERE last_seen_at IS NULL;")
            cur.execute("UPDATE deals SET active = TRUE WHERE active IS NULL;")

            conn.commit()


def fetch_deals(limit=200):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM deals
                WHERE active = TRUE
                ORDER BY score DESC NULLS LAST, est_profit DESC NULLS LAST
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
