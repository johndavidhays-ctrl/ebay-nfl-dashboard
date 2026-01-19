import os
from contextlib import contextmanager
from decimal import Decimal
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    return url


@contextmanager
def get_conn():
    conn = psycopg2.connect(_database_url())
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    """
    Creates the deals table if it does not exist.
    Adds new columns safely if the table already exists.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS deals (
                  item_id text PRIMARY KEY,
                  title text NOT NULL,
                  item_url text NOT NULL,
                  sold_url text NOT NULL,
                  buy_price numeric NOT NULL,
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

            cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_score ON deals (score DESC NULLS LAST);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_deals_profit ON deals (est_profit DESC NULLS LAST);")

            conn.commit()


def _to_decimal(x: Any) -> Decimal:
    if x is None:
        return Decimal("0")
    if isinstance(x, Decimal):
        return x
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal("0")


def fetch_deals(limit: int = 200) -> List[Dict[str, Any]]:
    """
    Returns rows for the dashboard.
    Sorts best opportunities first.
    """
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
                  created_at,
                  updated_at
                FROM deals
                ORDER BY
                  score DESC NULLS LAST,
                  est_profit DESC NULLS LAST,
                  roi DESC NULLS LAST,
                  created_at DESC
                LIMIT %s;
                """,
                (limit,),
            )
            rows = cur.fetchall()

    for r in rows:
        r["buy_price"] = float(_to_decimal(r.get("buy_price")))
        r["buy_shipping"] = float(_to_decimal(r.get("buy_shipping")))
        r["est_profit"] = float(_to_decimal(r.get("est_profit"))) if r.get("est_profit") is not None else None
        r["roi"] = float(_to_decimal(r.get("roi"))) if r.get("roi") is not None else None
        r["score"] = float(_to_decimal(r.get("score"))) if r.get("score") is not None else None

    return rows


def update_status(
    item_id: str,
    est_profit: Optional[float] = None,
    roi: Optional[float] = None,
    score: Optional[float] = None,
    listing_type: Optional[str] = None,
) -> None:
    """
    Optional helper if you want to update scoring later.
    Safe to keep even if you do not use it yet.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE deals
                SET
                  est_profit = COALESCE(%s, est_profit),
                  roi = COALESCE(%s, roi),
                  score = COALESCE(%s, score),
                  listing_type = COALESCE(%s, listing_type),
                  updated_at = now()
                WHERE item_id = %s;
                """,
                (est_profit, roi, score, listing_type, item_id),
            )
            conn.commit()
