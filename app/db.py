import os
import psycopg2
from contextlib import contextmanager


def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL env var is missing")
    return url


@contextmanager
def get_conn():
    conn = psycopg2.connect(_db_url(), sslmode="require")
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS deals (
                    item_id text PRIMARY KEY,
                    title text,
                    item_url text,
                    sold_url text,
                    buy_price numeric,
                    buy_shipping numeric,
                    created_at timestamptz DEFAULT now(),
                    est_profit numeric,
                    roi numeric,
                    score numeric,
                    listing_type text
                );
                """
            )

            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS title text;")
            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS item_url text;")
            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS sold_url text;")
            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS buy_price numeric;")
            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS buy_shipping numeric;")

            cur.execute(
                "ALTER TABLE deals ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now();"
            )
            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS est_profit numeric;")
            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS roi numeric;")
            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS score numeric;")
            cur.execute("ALTER TABLE deals ADD COLUMN IF NOT EXISTS listing_type text;")

            conn.commit()


def fetch_deals(limit: int = 200):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    item_id,
                    title,
                    item_url,
                    sold_url,
                    buy_price,
                    buy_shipping,
                    created_at,
                    est_profit,
                    roi,
                    score,
                    listing_type
                FROM deals
                ORDER BY score DESC NULLS LAST, created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()

    results = []
    for r in rows:
        results.append(
            {
                "item_id": r[0],
                "title": r[1],
                "item_url": r[2],
                "sold_url": r[3],
                "buy_price": float(r[4]) if r[4] is not None else None,
                "buy_shipping": float(r[5]) if r[5] is not None else None,
                "created_at": r[6].isoformat() if r[6] is not None else None,
                "est_profit": float(r[7]) if r[7] is not None else None,
                "roi": float(r[8]) if r[8] is not None else None,
                "score": float(r[9]) if r[9] is not None else None,
                "listing_type": r[10],
            }
        )
    return results


def update_status(item_id: str, est_profit=None, roi=None, score=None, listing_type=None):
    fields = []
    values = []

    if est_profit is not None:
        fields.append("est_profit = %s")
        values.append(est_profit)
    if roi is not None:
        fields.append("roi = %s")
        values.append(roi)
    if score is not None:
        fields.append("score = %s")
        values.append(score)
    if listing_type is not None:
        fields.append("listing_type = %s")
        values.append(listing_type)

    if not fields:
        return

    values.append(item_id)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE deals SET {', '.join(fields)} WHERE item_id = %s",
                tuple(values),
            )
            conn.commit()
