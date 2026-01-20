import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS deals (
                    id bigserial PRIMARY KEY,
                    item_id text UNIQUE NOT NULL,
                    title text NOT NULL,
                    item_url text NOT NULL,
                    sold_url text NOT NULL,
                    buy_price numeric NOT NULL DEFAULT 0,
                    buy_shipping numeric NOT NULL DEFAULT 0,

                    est_profit numeric,
                    roi numeric,
                    score numeric,
                    listing_type text,

                    created_at timestamptz NOT NULL DEFAULT now(),
                    updated_at timestamptz NOT NULL DEFAULT now()
                );
                """
            )

            cur.execute(
                """
                ALTER TABLE deals
                ADD COLUMN IF NOT EXISTS est_profit numeric,
                ADD COLUMN IF NOT EXISTS roi numeric,
                ADD COLUMN IF NOT EXISTS score numeric,
                ADD COLUMN IF NOT EXISTS listing_type text,
                ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
                ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();
                """
            )

            conn.commit()


def fetch_deals(limit=200):
    limit = int(limit)

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
                    listing_type
                FROM deals
                ORDER BY
                    COALESCE(score, -1e18) DESC,
                    COALESCE(est_profit, -1e18) DESC
                LIMIT %s;
                """,
                (limit,),
            )
            return cur.fetchall()
