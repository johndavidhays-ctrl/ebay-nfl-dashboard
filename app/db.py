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

            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP DEFAULT NOW();")
            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT TRUE;")

            conn.commit()
