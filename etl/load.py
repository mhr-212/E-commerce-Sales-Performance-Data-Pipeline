"""
Load stage: idempotent upsert of transformed data into PostgreSQL.
Uses INSERT … ON CONFLICT DO UPDATE (UPSERT) so re-runs are safe.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timezone
from typing import Optional
from uuid import UUID

import pandas as pd
from sqlalchemy import text

from etl.utils.db import get_engine, get_session
from etl.utils.logger import get_logger
from etl.utils.metrics import assert_count_variance, dataframe_checksum, write_audit

logger = get_logger(__name__)

# Chunk size for bulk inserts — tuned for PostgreSQL performance
_CHUNK_SIZE = 10_000

# Columns that map 1-to-1 to the fact_sales table
_FACT_COLS = [
    "sale_id", "date_key", "product_sk", "customer_sk",
    "sale_date", "order_id", "quantity", "unit_price",
    "discount_pct", "gross_amount", "discount_amount", "net_amount",
    "shipping_cost", "return_flag", "channel",
    "row_checksum", "pipeline_run_id", "loaded_at",
]

# ---------------------------------------------------------------------------
# Dimension loaders (called once during bootstrap or daily refresh)
# ---------------------------------------------------------------------------

def load_dim_products(df: pd.DataFrame, run_id: Optional[UUID] = None) -> int:
    """Upsert product dimension. Returns rows affected."""
    upsert_sql = text("""
        INSERT INTO dim_product
            (product_id, product_name, category, subcategory,
             brand, unit_cost, unit_price, is_active, valid_from, valid_to, row_hash)
        VALUES
            (:product_id, :product_name, :category, :subcategory,
             :brand, :unit_cost, :unit_price, TRUE,
             CURRENT_DATE, '9999-12-31',
             md5(CONCAT_WS('|', :product_id, :product_name, :unit_price::text)))
        ON CONFLICT (product_id, valid_from)
        DO UPDATE SET
            product_name = EXCLUDED.product_name,
            unit_cost    = EXCLUDED.unit_cost,
            unit_price   = EXCLUDED.unit_price,
            row_hash     = EXCLUDED.row_hash
    """)
    return _bulk_execute(upsert_sql, df)


def load_dim_customers(df: pd.DataFrame, run_id: Optional[UUID] = None) -> int:
    """Upsert customer dimension. Returns rows affected."""
    upsert_sql = text("""
        INSERT INTO dim_customer
            (customer_id, first_name, last_name, email, segment,
             country, region, city, signup_date)
        VALUES
            (:customer_id, :first_name, :last_name, :email, :segment,
             :country, :region, :city, :signup_date)
        ON CONFLICT (customer_id) DO UPDATE SET
            email   = EXCLUDED.email,
            segment = EXCLUDED.segment,
            city    = EXCLUDED.city
    """)
    return _bulk_execute(upsert_sql, df)


# ---------------------------------------------------------------------------
# Fact loader
# ---------------------------------------------------------------------------

def load(
    df: pd.DataFrame,
    run_date: date,
    run_id: Optional[UUID] = None,
    dag_id: str = "sales_etl",
    task_id: str = "load",
    source_checksum: Optional[str] = None,
) -> dict:
    """
    Idempotent upsert of transformed sales rows into fact_sales.
    Returns dict with rows_inserted, rows_updated, target_checksum.
    """
    if df.empty:
        logger.warning("[load] Received empty DataFrame — nothing to load.")
        return {"rows_inserted": 0, "rows_updated": 0, "target_checksum": ""}

    started_at = datetime.now(tz=timezone.utc)

    # Select only columns that exist in the target table
    available = [c for c in _FACT_COLS if c in df.columns]
    load_df   = df[available].copy()

    # Coerce return_flag to Python bool for psycopg2
    if "return_flag" in load_df.columns:
        load_df["return_flag"] = load_df["return_flag"].astype(bool)

    upsert_sql = text("""
        INSERT INTO fact_sales
            (sale_id, date_key, product_sk, customer_sk,
             sale_date, order_id, quantity, unit_price,
             discount_pct, gross_amount, discount_amount, net_amount,
             shipping_cost, return_flag, channel,
             row_checksum, pipeline_run_id, loaded_at)
        VALUES
            (:sale_id, :date_key, :product_sk, :customer_sk,
             :sale_date, :order_id, :quantity, :unit_price,
             :discount_pct, :gross_amount, :discount_amount, :net_amount,
             :shipping_cost, :return_flag, :channel,
             :row_checksum, :pipeline_run_id, :loaded_at)
        ON CONFLICT (sale_id, sale_date) DO UPDATE SET
            quantity         = EXCLUDED.quantity,
            net_amount       = EXCLUDED.net_amount,
            discount_pct     = EXCLUDED.discount_pct,
            return_flag      = EXCLUDED.return_flag,
            row_checksum     = EXCLUDED.row_checksum,
            pipeline_run_id  = EXCLUDED.pipeline_run_id,
            loaded_at        = EXCLUDED.loaded_at
        WHERE fact_sales.row_checksum != EXCLUDED.row_checksum
    """)

    rows_inserted = 0
    rows_updated  = 0
    n_chunks = math.ceil(len(load_df) / _CHUNK_SIZE)

    for i, chunk in enumerate(
        [load_df.iloc[j * _CHUNK_SIZE:(j + 1) * _CHUNK_SIZE] for j in range(n_chunks)]
    ):
        records = chunk.to_dict("records")
        try:
            with get_session() as session:
                result = session.execute(upsert_sql, records)
                # rowcount for UPSERT returns total affected rows
                rows_inserted += result.rowcount
            logger.info(f"[load] Chunk {i+1}/{n_chunks} done — {result.rowcount} rows affected")
        except Exception as exc:
            logger.error(f"[load] Chunk {i+1} failed: {exc}")
            _write_audit_safe(
                run_id, dag_id, task_id, run_date, "load", "failed",
                source_checksum=source_checksum, error_message=str(exc),
                started_at=started_at,
            )
            raise

    target_checksum = _compute_db_checksum(run_date)
    logger.info(
        f"[load] Complete — total affected rows: {rows_inserted} | "
        f"DB checksum={target_checksum[:12]}…"
    )

    _write_audit_safe(
        run_id, dag_id, task_id, run_date, "load", "success",
        rows_processed=len(load_df),
        rows_inserted=rows_inserted,
        source_checksum=source_checksum,
        target_checksum=target_checksum,
        started_at=started_at,
    )

    return {
        "rows_inserted":    rows_inserted,
        "target_checksum":  target_checksum,
    }


# ---------------------------------------------------------------------------
# Validation gate (called by Airflow validate_load task)
# ---------------------------------------------------------------------------

def validate_load(
    source_df: pd.DataFrame,
    run_date: date,
    tolerance_pct: float = 0.001,   # 0.1% tolerance
) -> None:
    """
    Assert that SUM(net_amount) in the DB for run_date equals source sum.
    Raises AssertionError if variance exceeds tolerance.
    """
    source_sum = float(source_df["net_amount"].sum())
    db_sum_rows = _query_db_sum(run_date)
    db_sum = db_sum_rows[0]["total_net"] if db_sum_rows else 0.0

    variance = abs(db_sum - source_sum) / max(source_sum, 1)
    logger.info(
        f"[validate_load] source={source_sum:,.2f} db={db_sum:,.2f} variance={variance:.4%}"
    )
    if variance > tolerance_pct:
        raise AssertionError(
            f"Load validation FAILED for {run_date}: "
            f"source_sum={source_sum:,.4f} db_sum={db_sum:,.4f} variance={variance:.4%}"
        )
    logger.info(f"[validate_load] ✅ Passed for {run_date}")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _bulk_execute(sql, df: pd.DataFrame) -> int:
    records = df.to_dict("records")
    with get_session() as session:
        result = session.execute(sql, records)
        return result.rowcount


def _compute_db_checksum(run_date: date) -> str:
    rows = _query_db_sum(run_date)
    if not rows:
        return ""
    payload = f"{run_date}|{rows[0]['total_net']}|{rows[0]['row_count']}"
    import hashlib
    return hashlib.sha256(payload.encode()).hexdigest()


def _query_db_sum(run_date: date) -> list:
    from etl.utils.db import execute_sql
    return execute_sql(
        "SELECT SUM(net_amount) AS total_net, COUNT(*) AS row_count "
        "FROM fact_sales WHERE sale_date = :d",
        {"d": run_date},
    )


def _write_audit_safe(run_id, dag_id, task_id, run_date, stage, status,
                      rows_processed=0, rows_inserted=0,
                      source_checksum=None, target_checksum=None,
                      error_message=None, started_at=None):
    if run_id is None:
        return
    try:
        write_audit(
            run_id=run_id, dag_id=dag_id, task_id=task_id,
            run_date=run_date, stage=stage, status=status,
            rows_processed=rows_processed, rows_inserted=rows_inserted,
            source_checksum=source_checksum, target_checksum=target_checksum,
            error_message=error_message, started_at=started_at,
        )
    except Exception as exc:
        logger.error(f"[load] Audit write failed (non-fatal): {exc}")
