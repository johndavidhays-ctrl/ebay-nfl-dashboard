import os
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Numeric,
    String,
    Text,
    create_engine,
    text,
)
from sqlalchemy.orm import declarative_base, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is missing")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

Base = declarative_base()


class Deal(Base):
    __tablename__ = "deals"

    item_id = Column(String, primary_key=True)
    title = Column(Text, nullable=False)
    url = Column(Text, nullable=True)
    image_url = Column(Text, nullable=True)
    query = Column(Text, nullable=True)

    total_cost = Column(Numeric(12, 2), nullable=False, server_default=text("0"))
    market = Column(Numeric(12, 2), nullable=False, server_default=text("0"))
    profit = Column(Numeric(12, 2), nullable=False, server_default=text("0"))

    ends_at = Column(DateTime(timezone=True), nullable=True)

    is_active = Column(Boolean, nullable=False, server_default=text("TRUE"))
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)


def mark_all_inactive() -> None:
    with SessionLocal() as db:
        db.execute(text("UPDATE deals SET is_active = FALSE"))
        db.commit()


def prune_inactive(older_than_days: int = 14) -> int:
    with SessionLocal() as db:
        q = text(
            """
            DELETE FROM deals
            WHERE is_active = FALSE
              AND updated_at < (NOW() AT TIME ZONE 'UTC') - (:days || ' days')::interval
            """
        )
        res = db.execute(q, {"days": older_than_days})
        db.commit()
        return int(res.rowcount or 0)


def upsert_deal(deal: dict[str, Any]) -> None:
    now = utcnow()
    with SessionLocal() as db:
        existing = db.get(Deal, deal["item_id"])
        if existing:
            existing.title = deal.get("title") or existing.title
            existing.url = deal.get("url")
            existing.image_url = deal.get("image_url")
            existing.query = deal.get("query")

            existing.total_cost = deal.get("total_cost", 0)
            existing.market = deal.get("market", 0)
            existing.profit = deal.get("profit", 0)

            existing.ends_at = deal.get("ends_at")
            existing.is_active = True
            existing.updated_at = now
        else:
            row = Deal(
                item_id=deal["item_id"],
                title=deal.get("title") or "",
                url=deal.get("url"),
                image_url=deal.get("image_url"),
                query=deal.get("query"),
                total_cost=deal.get("total_cost", 0),
                market=deal.get("market", 0),
                profit=deal.get("profit", 0),
                ends_at=deal.get("ends_at"),
                is_active=True,
                created_at=now,
                updated_at=now,
            )
            db.add(row)

        db.commit()


def fetch_active_deals(limit: int = 200) -> list[dict[str, Any]]:
    with SessionLocal() as db:
        rows = (
            db.query(Deal)
            .filter(Deal.is_active.is_(True))
            .order_by(Deal.ends_at.asc().nullslast())
            .limit(limit)
            .all()
        )

        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "item_id": r.item_id,
                    "title": r.title,
                    "url": r.url,
                    "image_url": r.image_url,
                    "query": r.query,
                    "total_cost": float(r.total_cost or 0),
                    "market": float(r.market or 0),
                    "profit": float(r.profit or 0),
                    "ends_at": r.ends_at.isoformat() if r.ends_at else None,
                }
            )
        return out
