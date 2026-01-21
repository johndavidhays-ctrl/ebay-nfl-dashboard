import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import Boolean, DateTime, Float, Integer, String, Text, create_engine, select, delete
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Deal(Base):
    __tablename__ = "deals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    item_id: Mapped[str] = mapped_column(String(40), unique=True, index=True)
    title: Mapped[str] = mapped_column(Text)
    url: Mapped[str] = mapped_column(Text)
    image_url: Mapped[str] = mapped_column(Text, default="")
    query: Mapped[str] = mapped_column(String(200), default="")

    total_cost: Mapped[float] = mapped_column(Float, default=0.0)
    market: Mapped[float] = mapped_column(Float, default=0.0)
    profit: Mapped[float] = mapped_column(Float, default=0.0)

    end_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    first_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


def get_engine():
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is missing in environment")
    return create_engine(url, pool_pre_ping=True)


def init_db():
    eng = get_engine()
    Base.metadata.create_all(eng)
    return eng


def mark_all_inactive(session: Session):
    session.query(Deal).update({Deal.active: False})
    session.commit()


def upsert_deal(session: Session, payload: Dict[str, Any]):
    item_id = payload["item_id"]
    existing = session.scalar(select(Deal).where(Deal.item_id == item_id))
    now = _utcnow()

    if existing:
        existing.title = payload["title"]
        existing.url = payload["url"]
        existing.image_url = payload.get("image_url", "") or ""
        existing.query = payload.get("query", "") or ""
        existing.total_cost = float(payload.get("total_cost", 0.0))
        existing.market = float(payload.get("market", 0.0))
        existing.profit = float(payload.get("profit", 0.0))
        existing.end_time = payload.get("end_time")
        existing.active = True
        existing.last_seen = now
    else:
        d = Deal(
            item_id=item_id,
            title=payload["title"],
            url=payload["url"],
            image_url=payload.get("image_url", "") or "",
            query=payload.get("query", "") or "",
            total_cost=float(payload.get("total_cost", 0.0)),
            market=float(payload.get("market", 0.0)),
            profit=float(payload.get("profit", 0.0)),
            end_time=payload.get("end_time"),
            active=True,
            first_seen=now,
            last_seen=now,
        )
        session.add(d)

    session.commit()


def prune_inactive(session: Session):
    session.execute(delete(Deal).where(Deal.active == False))  # noqa: E712
    session.commit()


def fetch_active_deals(session: Session, limit: int = 200) -> List[Deal]:
    stmt = (
        select(Deal)
        .where(Deal.active == True)  # noqa: E712
        .order_by(Deal.end_time.asc().nullslast(), Deal.profit.desc())
        .limit(limit)
    )
    return list(session.scalars(stmt).all())
