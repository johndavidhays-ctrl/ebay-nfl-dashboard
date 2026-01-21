import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

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


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is missing")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)


class Base(DeclarativeBase):
    pass


class Item(Base):
    __tablename__ = "items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ebay_item_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)

    title: Mapped[str] = mapped_column(String(512))
    url: Mapped[str] = mapped_column(String(2048))
    image_url: Mapped[str] = mapped_column(String(2048))

    lane: Mapped[str] = mapped_column(String(16), default="graded")  # graded or raw

    currency: Mapped[str] = mapped_column(String(8), default="USD")
    total_price: Mapped[float] = mapped_column(Float, default=0.0)
    market_value: Mapped[float] = mapped_column(Float, default=0.0)
    profit: Mapped[float] = mapped_column(Float, default=0.0)

    end_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    active: Mapped[bool] = mapped_column(Boolean, default=True)

    raw_json: Mapped[str] = mapped_column(Text, default="{}")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    __table_args__ = (
        Index("idx_items_lane_profit", "lane", "profit"),
        Index("idx_items_end_time", "end_time"),
        Index("idx_items_active", "active"),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ebay_item_id": self.ebay_item_id,
            "title": self.title,
            "url": self.url,
            "image_url": self.image_url,
            "lane": self.lane,
            "currency": self.currency,
            "total_price": float(self.total_price),
            "market_value": float(self.market_value),
            "profit": float(self.profit),
            "end_time": self.end_time.isoformat() if self.end_time else None,
        }


def ensure_schema() -> None:
    Base.metadata.create_all(bind=engine)

    # Add lane column if an older table exists
    with engine.begin() as conn:
        cols = conn.execute(
            text(
                """
                select column_name
                from information_schema.columns
                where table_name = 'items'
                """
            )
        ).fetchall()
        colnames = {c[0] for c in cols}
        if "lane" not in colnames:
            conn.execute(text("alter table items add column lane varchar(16) default 'graded'"))
