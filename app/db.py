import os
from datetime import datetime, timezone

from sqlalchemy import (
    create_engine,
    text,
)

DATABASE_URL = os.getenv("DATABASE_URL")


def _must_get_database_url() -> str:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing in environment variables")
    return DATABASE_URL


def get_engine():
    url = _must_get_database_url()
    return create_engine(url, pool_pre_ping=True)


def init_db():
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS deals (item_id TEXT UNIQUE)"))

        row = conn.execute(
            text(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'deals' AND column_name = 'id'
                LIMIT 1
                """
            )
        ).fetchone()

        if not row:
            conn.execute(text("ALTER TABLE deals ADD COLUMN id BIGSERIAL"))
            conn.execute(text("ALTER TABLE deals ADD PRIMARY KEY (id)"))

        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS item_id TEXT"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS title TEXT"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS url TEXT"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS image_url TEXT"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS query TEXT"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS total_cost NUMERIC"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS market NUMERIC"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS profit NUMERIC"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS end_time TIMESTAMPTZ"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ"))
        conn.execute(text("ALTER TABLE deals ADD COLUMN IF NOT EXISTS is_active BOOLEAN"))

        conn.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_deals_item_id
                ON deals (item_id)
                """
            )
        )

        now = datetime.now(timezone.utc)
        conn.execute(
            text(
                """
                UPDATE deals
                SET is_active = false, updated_at = :now
                WHERE is_active IS DISTINCT FROM false
                """
            ),
            {"now": now},
        )

    return eng
