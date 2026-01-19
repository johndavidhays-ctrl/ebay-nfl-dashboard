import os
import psycopg2
import psycopg2.extras

def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])

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
                  seller_feedback_percent NUMERIC,
                  seller_feedback_score INTEGER,
                  status TEXT DEFAULT 'new',
                  scanned_utc TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.commit()

def fetch_deals(limit=200):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
              SELECT * FROM deals
              ORDER BY scanned_utc DESC
              LIMIT %s
            """, (limit,))
            return cur.fetchall()

def update_status(item_id, status):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
              UPDATE deals SET status=%s WHERE item_id=%s
            """, (status, item_id))
            conn.commit()
