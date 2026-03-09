"""
Shared pytest fixtures:
  - sample_df:      100-row in-memory sales DataFrame
  - bad_df:         DataFrame with intentional validation errors
  - sqlite_engine:  In-memory SQLite engine (for load integration tests)
  - tmp_csv:        Writes sample_df to a temp CSV file
  - products_df:    Slim dim_product DataFrame with product_id → product_sk mapping
  - customers_df:   Slim dim_customer DataFrame with customer_id → customer_sk mapping
"""
from __future__ import annotations

import uuid
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text


# ---------------------------------------------------------------------------
# Helper: build a canonical sales row
# ---------------------------------------------------------------------------

def _make_row(
    i: int,
    sale_date: date = date(2023, 6, 15),
    product_id: str = "PROD-00001",
    customer_id: str = "CUST-0000001",
    quantity: int = 2,
    unit_price: float = 100.0,
    discount_pct: float = 0.10,
    return_flag: bool = False,
    channel: str = "online",
) -> dict:
    gross    = round(quantity * unit_price, 4)
    disc_amt = round(gross * discount_pct, 4)
    net      = round(gross - disc_amt, 4)
    return {
        "sale_id":         str(uuid.uuid4()),
        "order_id":        f"ORD-{i:05d}",
        "sale_date":       sale_date,
        "product_id":      product_id,
        "customer_id":     customer_id,
        "quantity":        quantity,
        "unit_price":      unit_price,
        "discount_pct":    discount_pct,
        "gross_amount":    gross,
        "discount_amount": disc_amt,
        "net_amount":      net,
        "shipping_cost":   5.00,
        "return_flag":     return_flag,
        "channel":         channel,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_df() -> pd.DataFrame:
    """100 clean, valid sales rows."""
    rows = [_make_row(i) for i in range(100)]
    df = pd.DataFrame(rows)
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    return df


@pytest.fixture
def bad_df() -> pd.DataFrame:
    """DataFrame with deliberate schema violations for testing dead-letter routing."""
    rows = [_make_row(i) for i in range(10)]
    df = pd.DataFrame(rows)
    df.loc[0, "quantity"]       = -5          # invalid: must be > 0
    df.loc[1, "discount_pct"]   = 1.5         # invalid: must be <= 1
    df.loc[2, "net_amount"]     = float("nan")
    df.loc[3, "channel"]        = "pigeon_post"  # invalid channel
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    return df


@pytest.fixture
def products_df() -> pd.DataFrame:
    """Minimal dim_product slice with surrogate keys."""
    return pd.DataFrame([
        {"product_id": "PROD-00001", "product_sk": 1, "is_active": True},
        {"product_id": "PROD-00002", "product_sk": 2, "is_active": True},
        {"product_id": "PROD-00003", "product_sk": 3, "is_active": True},
    ])


@pytest.fixture
def customers_df() -> pd.DataFrame:
    """Minimal dim_customer slice with surrogate keys."""
    return pd.DataFrame([
        {"customer_id": "CUST-0000001", "customer_sk": 101},
        {"customer_id": "CUST-0000002", "customer_sk": 102},
    ])


@pytest.fixture
def sqlite_engine():
    """
    In-memory SQLite engine with a simplified fact_sales schema for load tests.
    Yields the engine and tears down afterward.
    """
    engine = create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_sales (
                sale_id          TEXT NOT NULL,
                date_key         INTEGER NOT NULL,
                product_sk       INTEGER,
                customer_sk      INTEGER,
                sale_date        TEXT NOT NULL,
                order_id         TEXT NOT NULL,
                quantity         INTEGER NOT NULL,
                unit_price       REAL NOT NULL,
                discount_pct     REAL NOT NULL DEFAULT 0,
                gross_amount     REAL NOT NULL,
                discount_amount  REAL NOT NULL DEFAULT 0,
                net_amount       REAL NOT NULL,
                shipping_cost    REAL NOT NULL DEFAULT 0,
                return_flag      INTEGER NOT NULL DEFAULT 0,
                channel          TEXT NOT NULL DEFAULT 'online',
                row_checksum     TEXT NOT NULL DEFAULT '',
                pipeline_run_id  TEXT NOT NULL DEFAULT '',
                loaded_at        TEXT,
                PRIMARY KEY (sale_id, sale_date)
            )
        """))
    yield engine
    engine.dispose()


@pytest.fixture
def tmp_csv(tmp_path: Path, sample_df: pd.DataFrame) -> Path:
    """Write sample_df to a temporary CSV and return the file path."""
    fpath = tmp_path / "sales_2023_06.csv"
    sample_df.to_csv(fpath, index=False)
    return fpath
