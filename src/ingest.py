from __future__ import annotations

import csv
import glob
import logging
import os
import uuid
from datetime import datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from .models import Activity

logger = logging.getLogger(__name__)

KNOWN_PRODUCTS: frozenset[str] = frozenset(
    {"POS", "AIRTIME", "BILLS", "CARD_PAYMENT", "SAVINGS", "MONIEBOOK", "KYC"}
)
KNOWN_STATUSES: frozenset[str] = frozenset({"SUCCESS", "FAILED", "PENDING"})

_BATCH_SIZE = 10_000


def _parse_timestamp(raw: str) -> datetime | None:
    """Parse an ISO 8601 timestamp string, handling trailing ``Z`` offsets."""
    if not raw:
        return None
    raw = raw.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except (ValueError, TypeError):
        return None


def _parse_amount(raw: str) -> Decimal:
    """Parse a numeric string into a Decimal, defaulting to 0.00 on failure."""
    if not raw:
        return Decimal("0.00")
    raw = raw.strip().replace(",", "")
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return Decimal("0.00")


def _clean(value: str | None, *, upper: bool = False) -> str | None:
    """Strip whitespace from *value*, optionally uppercase it, return None if empty."""
    if not value:
        return None
    cleaned = value.strip()
    if upper:
        cleaned = cleaned.upper()
    return cleaned or None


def ingest_csv_dir(db: Session, data_dir: str) -> dict[str, int]:
    """Import all activity CSV files from *data_dir* into the database.

    Args:
        db:       Active SQLAlchemy session (caller is responsible for commit).
        data_dir: Path to the directory containing ``activities_*.csv`` files.

    Returns:
        A summary dict with keys ``inserted``, ``skipped``, and ``files``.
    """
    csv_files = sorted(glob.glob(os.path.join(data_dir, "activities_*.csv")))
    total_inserted = 0
    total_skipped = 0

    for file_path in csv_files:
        inserted, skipped = _ingest_file(db, file_path)
        total_inserted += inserted
        total_skipped += skipped
        logger.info("Processed %s — inserted=%d skipped=%d", file_path, inserted, skipped)

    db.commit()
    logger.info(
        "Ingestion complete — total inserted=%d skipped=%d files=%d",
        total_inserted, total_skipped, len(csv_files),
    )
    return {"inserted": total_inserted, "skipped": total_skipped, "files": len(csv_files)}



def _ingest_file(db: Session, file_path: str) -> tuple[int, int]:
    """Parse a single CSV file and bulk-upsert its valid rows.

    Returns:
        A ``(inserted, skipped)`` tuple for this file.
    """
    inserted = 0
    skipped = 0
    batch: list[dict] = []

    with open(file_path, encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for raw_row in reader:
            record = _parse_row(raw_row)
            if record is None:
                skipped += 1
                continue

            batch.append(record)
            if len(batch) >= _BATCH_SIZE:
                inserted += _bulk_upsert(db, batch)
                batch.clear()

    if batch:
        inserted += _bulk_upsert(db, batch)

    return inserted, skipped


def _parse_row(row: dict) -> dict | None:
    """Validate and normalise a single CSV row dict.

    Returns ``None`` for any row that should be skipped.
    """
    try:
        event_id = uuid.UUID((row.get("event_id") or "").strip())
    except (ValueError, AttributeError):
        return None

    merchant_id = _clean(row.get("merchant_id"))
    if not merchant_id:
        return None

    timestamp = _parse_timestamp(row.get("event_timestamp") or "")
    if timestamp is None:
        return None

    product = _clean(row.get("product"), upper=True)
    status = _clean(row.get("status"), upper=True)

    if product not in KNOWN_PRODUCTS or status not in KNOWN_STATUSES:
        return None

    return {
        "event_id": event_id,
        "merchant_id": merchant_id,
        "event_timestamp": timestamp,
        "product": product,
        "event_type": _clean(row.get("event_type"), upper=True),
        "amount": _parse_amount(row.get("amount") or "0"),
        "status": status,
        "channel": _clean(row.get("channel"), upper=True),
        "region": _clean(row.get("region")),
        "merchant_tier": _clean(row.get("merchant_tier"), upper=True),
    }


def _bulk_upsert(db: Session, rows: list[dict]) -> int:
    """Insert *rows* into the activities table, ignoring duplicates by ``event_id``.

    Returns the number of rows actually inserted.
    """
    stmt = pg_insert(Activity).values(rows)
    stmt = stmt.on_conflict_do_nothing(index_elements=[Activity.event_id])
    result = db.execute(stmt)
    return result.rowcount or 0
