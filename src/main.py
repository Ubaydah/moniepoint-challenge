from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import Depends, FastAPI
from sqlalchemy import select
from sqlalchemy.orm import Session

from .analytics import (
    get_failure_rates,
    get_kyc_funnel,
    get_monthly_active_merchants,
    get_product_adoption,
    get_top_merchant,
)
from .db import Base, SessionLocal, engine, get_db
from .ingest import ingest_csv_dir
from .models import Activity
from .settings import settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan: create tables and optionally ingest CSV data on startup."""
    Base.metadata.create_all(bind=engine)

    if settings.import_on_startup:
        with SessionLocal() as db:
            has_data = db.execute(select(Activity.event_id).limit(1)).first() is not None
            if not has_data or settings.force_import:
                logger.info("Starting CSV ingestion from '%s'...", settings.data_dir)
                summary = ingest_csv_dir(db, settings.data_dir)
                logger.info(
                    "Ingestion complete: inserted=%d skipped=%d files=%d",
                    summary["inserted"],
                    summary["skipped"],
                    summary["files"],
                )
            else:
                logger.info("Data already present — skipping ingestion (set FORCE_IMPORT=true to override).")

    yield


app = FastAPI(
    title="Moniepoint Analytics API Challenge",
    description="Merchant activity analytics over a year of transaction data.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/analytics/top-merchant", summary="Merchant with highest successful transaction volume")
def top_merchant(db: Session = Depends(get_db)):
    return get_top_merchant(db)


@app.get("/analytics/monthly-active-merchants", summary="Unique active merchants per calendar month")
def monthly_active_merchants(db: Session = Depends(get_db)):
    return get_monthly_active_merchants(db)


@app.get("/analytics/product-adoption", summary="Unique merchant count per product")
def product_adoption(db: Session = Depends(get_db)):
    return get_product_adoption(db)


@app.get("/analytics/kyc-funnel", summary="KYC conversion funnel (successful events only)")
def kyc_funnel(db: Session = Depends(get_db)):
    return get_kyc_funnel(db)


@app.get("/analytics/failure-rates", summary="Failure rate per product, sorted descending")
def failure_rates(db: Session = Depends(get_db)):
    return get_failure_rates(db)
