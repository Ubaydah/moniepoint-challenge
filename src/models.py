from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import DateTime, Index, Numeric, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


class Activity(Base):
    """Represents a single merchant activity event."""

    __tablename__ = "activities"

    event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True
    )
    merchant_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    event_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )

    product: Mapped[str] = mapped_column(String, nullable=False, index=True)
    event_type: Mapped[str | None] = mapped_column(String, nullable=True)

    amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 2), nullable=False, default=Decimal("0.00")
    )
    status: Mapped[str] = mapped_column(String, nullable=False, index=True)

    channel: Mapped[str | None] = mapped_column(String, nullable=True)
    region: Mapped[str | None] = mapped_column(String, nullable=True)
    merchant_tier: Mapped[str | None] = mapped_column(String, nullable=True)

    __table_args__ = (
        Index("idx_status_product", "status", "product"),
    )
