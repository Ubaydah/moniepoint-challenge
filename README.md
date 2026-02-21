# Moniepoint Code Challenge — Analytics Merchant API

**Candidate:** Ubaydah Abdulwasiu

## Project Summary

A REST API built with **FastAPI** + **SQLAlchemy 2.x** + **PostgreSQL** that ingests daily merchant activity CSV files and exposes five analytics endpoints on port `8080`.

---

## Assumptions

1. **Data directory** — CSV files are expected at `./data/activities_YYYYMMDD.csv` relative to the project root. The path is configurable via the `DATA_DIR` env var.
2. **Idempotent ingestion** — On startup the API checks whether the `activities` table already has rows. If it does, ingestion is skipped (no duplicate imports). Set `FORCE_IMPORT=true` to re-import.
3. **Malformed rows** — Rows with missing `merchant_id`, unparseable timestamps, unknown product values, or invalid UUIDs are silently skipped and counted as `skipped`.
4. **Product adoption** — "Unique merchant count per product" counts merchants regardless of event status (no SUCCESS filter), matching the spec wording.
5. **Failure rate** — Only `SUCCESS` and `FAILED` statuses contribute; `PENDING` events are excluded from both numerator and denominator.
6. **Monetary rounding** — Amounts are stored as `NUMERIC(18,2)` in PostgreSQL; the API returns `float` values already rounded to 2 decimal places.
7. **Month gap-filling** — `/analytics/monthly-active-merchants` fills every month between the data's min and max timestamp with `0` for months with no active merchants.
8. **Timezone** — All timestamps are stored with timezone info. The `date_trunc('month', ...)` groups by UTC month.

---

## Tech Stack

| Layer     | Choice          |
| --------- | --------------- |
| Language  | Python 3.11+    |
| Framework | FastAPI         |
| ORM       | SQLAlchemy 2.x  |
| Database  | PostgreSQL 14+  |
| Driver    | psycopg2-binary |

---

## Prerequisites

- Python 3.11+
- PostgreSQL 14+ running locally (or via Docker)
- `pip` or a virtual environment manager

---

## Setup & Run

### 1. Clone & install dependencies

```bash
git clone <your-repo-url>
cd moniepoint-challenge
python3 -m venv env
source env/bin/activate        # Windows: env\Scripts\activate
pip3 install -r requirements.txt
```

### 2. Configure the database

Copy `.env` and fill in your PostgreSQL credentials:

```bash
cp .env .env.local   # optional
```

Create the database if it doesn't exist:

```bash
psql -U postgres -c "CREATE DATABASE moniepoint;"
```

### 3. Place CSV data

```
moniepoint-challenge/
└── data/
    ├── activities_20240101.csv
    ├── activities_20240102.csv
    └── ...
```

### 4. Start the API

```bash
uvicorn src.main:app --host 0.0.0.0 --port 8080
```

On first startup, the API will:

1. Create the `activities` table (with indexes)
2. Import all CSV files from `./data/`

Import of a full year of data takes **under 5 minutes** on typical hardware using batch upserts of 10,000 rows.

---

## Environment Variables

| Variable            | Default      | Description                   |
| ------------------- | ------------ | ----------------------------- |
| `DATABASE_NAME`     | `moniepoint` | PostgreSQL database name      |
| `DATABASE_USER`     | `postgres`   | PostgreSQL user               |
| `DATABASE_PASSWORD` | `postgres`   | PostgreSQL password           |
| `DATABASE_HOST`     | `localhost`  | PostgreSQL host               |
| `DATABASE_PORT`     | `5432`       | PostgreSQL port               |
| `DATA_DIR`          | `data`       | Path to CSV directory         |
| `IMPORT_ON_STARTUP` | `true`       | Auto-import CSVs on startup   |
| `FORCE_IMPORT`      | `false`      | Re-import even if data exists |

---

## API Endpoints

All endpoints return `application/json` on port `8080`.

```bash
# Merchant with highest total successful transaction volume
curl http://localhost:8080/analytics/top-merchant

# Unique merchants with at least one success per month
curl http://localhost:8080/analytics/monthly-active-merchants

# Unique merchant count per product (sorted descending)
curl http://localhost:8080/analytics/product-adoption

# KYC conversion funnel (success events only)
curl http://localhost:8080/analytics/kyc-funnel

# Failure rate per product, sorted descending
curl http://localhost:8080/analytics/failure-rates
```

The api documentation can be found at `http://localhost:8080/docs`

---

## Running Tests

To run the test

```bash
pip install pytest
pytest tests/ -v
```

---
