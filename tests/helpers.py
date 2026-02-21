import uuid as _uuid
from datetime import datetime, timezone
from decimal import Decimal
from sqlalchemy import event as sa_event
from src.models import Activity  


def register_sqlite_extensions(engine):
    """Register Python-implemented UDFs so SQLite can handle our queries."""

    @sa_event.listens_for(engine, "connect")
    def _connect(dbapi_conn, _):
        dbapi_conn.create_function("date_trunc", 2, _sqlite_date_trunc)
        dbapi_conn.create_function("nullif", 2, _sqlite_nullif)


def _sqlite_date_trunc(unit: str, value: str) -> str:
    """Minimal date_trunc shim: truncates an ISO timestamp to 'month'."""
    if value is None:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
        if unit == "month":
            return datetime(dt.year, dt.month, 1).isoformat()
        return value
    except Exception:
        return value


def _sqlite_nullif(a, b):
    return None if a == b else a


def _ts(year: int, month: int, day: int = 1) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


def act(
    *,
    merchant_id: str,
    product: str,
    status: str,
    amount: float = 0.0,
    event_type: str = "CARD_TRANSACTION",
    event_timestamp: datetime = None,
    channel: str = "APP",
    region: str = "NORTH",
    merchant_tier: str = "STARTER",
) -> Activity:
    return Activity(
        event_id=_uuid.uuid4(),
        merchant_id=merchant_id,
        product=product,
        status=status,
        amount=Decimal(str(amount)),
        event_type=event_type,
        event_timestamp=event_timestamp or _ts(2024, 1),
        channel=channel,
        region=region,
        merchant_tier=merchant_tier,
    )
