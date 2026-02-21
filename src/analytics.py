from datetime import datetime
from sqlalchemy import select, func, distinct, case, desc, and_
from sqlalchemy.orm import Session
from .models import Activity

KNOWN_PRODUCTS = ["POS","AIRTIME","BILLS","CARD_PAYMENT","SAVINGS","MONIEBOOK","KYC"]

def _month_key(dt) -> str:
    """Return 'YYYY-MM' from a datetime or an ISO timestamp string."""
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    return f"{dt.year:04d}-{dt.month:02d}"

def _month_range(start: datetime, end: datetime) -> list[str]:
    y, m = start.year, start.month
    end_y, end_m = end.year, end.month
    out = []
    while (y < end_y) or (y == end_y and m <= end_m):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out

def get_top_merchant(db: Session):
    query = (
        select(
            Activity.merchant_id,
            func.round(func.sum(Activity.amount), 2).label("total_volume")
        )
        .where(Activity.status == "SUCCESS")
        .group_by(Activity.merchant_id)
        .order_by(desc(func.sum(Activity.amount)))
        .limit(1)
    )
    row = db.execute(query).first()
    if not row:
        return {"merchant_id": None, "total_volume": 0.00}
    return {"merchant_id": row[0], "total_volume": float(row[1])}

def get_monthly_active_merchants(db: Session):
    minmax = db.execute(
        select(func.min(Activity.event_timestamp), func.max(Activity.event_timestamp))
        .where(Activity.status == "SUCCESS")
    ).one()

    if not minmax[0] or not minmax[1]:
        return {}

    months = _month_range(minmax[0], minmax[1])

    month_expr = func.date_trunc("month", Activity.event_timestamp)
    query = (
        select(month_expr.label("m"), func.count(distinct(Activity.merchant_id)).label("cnt"))
        .where(Activity.status == "SUCCESS")
        .group_by(month_expr)
        .order_by(month_expr)
    )
    rows = db.execute(query).all()

    counts = {_month_key(m): int(cnt) for (m, cnt) in rows}
    return {k: counts.get(k, 0) for k in months}

def get_product_adoption(db: Session):
    query = (
        select(Activity.product, func.count(distinct(Activity.merchant_id)).label("cnt"))
        .group_by(Activity.product)
    )
    rows = db.execute(query).all()

    counts = {p: 0 for p in KNOWN_PRODUCTS}
    for product, cnt in rows:
        if product in counts:
            counts[product] = int(cnt)

    ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return {k: v for k, v in ordered}

def get_kyc_funnel(db: Session):
    docs = func.count(distinct(case((Activity.event_type == "DOCUMENT_SUBMITTED", Activity.merchant_id)))).label("documents_submitted")
    ver  = func.count(distinct(case((Activity.event_type == "VERIFICATION_COMPLETED", Activity.merchant_id)))).label("verifications_completed")
    upg  = func.count(distinct(case((Activity.event_type == "TIER_UPGRADE", Activity.merchant_id)))).label("tier_upgrades")

    query = select(docs, ver, upg).where(
        and_(Activity.product == "KYC", Activity.status == "SUCCESS")
    )
    row = db.execute(query).one()
    return {"documents_submitted": int(row[0]), "verifications_completed": int(row[1]), "tier_upgrades": int(row[2])}

def get_failure_rates(db):
    success = func.count().filter(Activity.status == "SUCCESS").label("success")
    failed  = func.count().filter(Activity.status == "FAILED").label("failed")

    subq = (
        select(Activity.product.label("product"), success, failed)
        .where(Activity.status.in_(("SUCCESS", "FAILED")))
        .group_by(Activity.product)
        .subquery()
    )

    denom = func.nullif(subq.c.failed + subq.c.success, 0)
    rate = func.round((subq.c.failed * 100.0) / denom, 1).label("failure_rate")

    query = (
        select(subq.c.product, rate)
        .where((subq.c.failed + subq.c.success) > 0)
        .order_by(desc("failure_rate"), subq.c.product)
    )

    rows = db.execute(query).all()
    return [{"product": p, "failure_rate": float(r)} for p, r in rows]
