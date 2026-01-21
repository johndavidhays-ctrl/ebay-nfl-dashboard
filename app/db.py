# app/db.py
import os
import json
from datetime import datetime, timezone
from typing import Optional, Any, Dict

from sqlalchemy import (
    create_engine,
    String,
    Integer,
    Boolean,
    DateTime,
    Float,
    Text,
    Index,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is missing")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "10")),
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


class Base(DeclarativeBase):
    pass


class Item(Base):
    __tablename__ = "items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    ebay_item_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(512), default="")
    url: Mapped[str] = mapped_column(String(2048), default="")
    image_url: Mapped[str] = mapped_column(String(2048), default="")

    query: Mapped[str] = mapped_column(String(128), default="")

    currency: Mapped[str] = mapped_column(String(8), default="USD")
    price: Mapped[float] = mapped_column(Float, default=0.0)
    shipping: Mapped[float] = mapped_column(Float, default=0.0)
    total_price: Mapped[float] = mapped_column(Float, default=0.0)

    end_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    condition: Mapped[str] = mapped_column(String(128), default="")
    seller: Mapped[str] = mapped_column(String(256), default="")

    market_value: Mapped[float] = mapped_column(Float, default=0.0)
    profit: Mapped[float] = mapped_column(Float, default=0.0)

    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)

    raw_json: Mapped[str] = mapped_column(Text, default="{}")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)

    __table_args__ = (
        Index("idx_items_active_total_price", "active", "total_price"),
        Index("idx_items_active_end_time", "active", "end_time"),
        Index("idx_items_active_profit", "active", "profit"),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "ebay_item_id": self.ebay_item_id,
            "title": self.title,
            "url": self.url,
            "image_url": self.image_url,
            "query": self.query,
            "currency": self.currency,
            "price": float(self.price or 0.0),
            "shipping": float(self.shipping or 0.0),
            "total_price": float(self.total_price or 0.0),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "condition": self.condition,
            "seller": self.seller,
            "market_value": float(self.market_value or 0.0),
            "profit": float(self.profit or 0.0),
            "active": bool(self.active),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    def set_raw(self, payload: Any) -> None:
        try:
            self.raw_json = json.dumps(payload, ensure_ascii=False, default=str)
        except Exception:
            self.raw_json = "{}"


def _column_exists(conn, table: str, column: str) -> bool:
    q = text(
        """
        select 1
        from information_schema.columns
        where table_name = :t and column_name = :c
        limit 1
        """
    )
    r = conn.execute(q, {"t": table, "c": column}).fetchone()
    return bool(r)


def ensure_schema() -> None:
    Base.metadata.create_all(bind=engine)

    with engine.begin() as conn:
        if not _column_exists(conn, "items", "market_value"):
            conn.execute(text("alter table items add column market_value double precision default 0"))
        if not _column_exists(conn, "items", "profit"):
            conn.execute(text("alter table items add column profit double precision default 0"))
