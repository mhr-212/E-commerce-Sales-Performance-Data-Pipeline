"""
Transform stage: cleanse, normalise, derive business metrics,
resolve dimension surrogate keys, compute row checksums.
"""
from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone
from typing import Optional
from uuid import UUID

import pandas as pd

from etl.utils.logger import get_logger
from etl.utils.metrics import assert_count_variance, dataframe_checksum, write_audit

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def transform(
    df: pd.DataFrame,
    run_date: date,
    products_df: Optional[pd.DataFrame] = None,
    customers_df: Optional[pd.DataFrame] = None,
    run_id: Optional[UUID] = None,
    dag_id: str = "sales_etl",
    task_id: str = "transform",
) -> pd.DataFrame:
    """
    Full transform pipeline:
      1. Copy → normalise dtypes
      2. Derive / validate financial metrics
      3. Resolve surrogate keys from dimension dataframes (or DB)
      4. Attach pipeline metadata
      5. Emit row-level checksums

    Returns DataFrame ready for load().
    """
    if df.empty:
        logger.warning("[transform] Empty input DataFrame — nothing to transform.")
        return df

    started_at = datetime.now(tz=timezone.utc)
    pre_count  = len(df)
    logger.info(f"[transform] Starting with {pre_count:,} rows")

    result = (
        df.copy()
          .pipe(_normalise_dtypes)
          .pipe(_derive_financial_metrics)
          .pipe(_validate_business_rules)
    )

    # Resolve surrogate keys (optional if dims are provided as DataFrames)
    if products_df is not None and not products_df.empty:
        result = _resolve_product_sk(result, products_df)
    if customers_df is not None and not customers_df.empty:
        result = _resolve_customer_sk(result, customers_df)

    result = _attach_date_key(result)
    result = _attach_pipeline_metadata(result, run_id)
    result = _compute_row_checksums(result)

    post_count = len(result)
    assert_count_variance(pre_count, post_count, max_pct=0.05, label="transform")

    checksum = dataframe_checksum(result, ["sale_id", "net_amount"])
    logger.info(f"[transform] Complete: {post_count:,} rows | checksum={checksum[:12]}…")

    if run_id:
        try:
            write_audit(
                run_id=run_id, dag_id=dag_id, task_id=task_id,
                run_date=run_date, stage="transform", status="success",
                rows_processed=pre_count,
                rows_inserted=post_count,
                source_checksum=checksum,
                started_at=started_at,
            )
        except Exception as exc:
            logger.error(f"[transform] Audit write failed: {exc}")

    return result


# ---------------------------------------------------------------------------
# Step functions (composable via .pipe())
# ---------------------------------------------------------------------------

def _normalise_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all columns have correct Python / Pandas types."""
    df["sale_date"]       = pd.to_datetime(df["sale_date"]).dt.date
    df["quantity"]        = df["quantity"].astype(int)
    df["unit_price"]      = df["unit_price"].astype(float).round(4)
    df["discount_pct"]    = df["discount_pct"].astype(float).round(4)
    df["gross_amount"]    = df["gross_amount"].astype(float).round(4)
    df["discount_amount"] = df["discount_amount"].astype(float).round(4)
    df["net_amount"]      = df["net_amount"].astype(float).round(4)
    df["shipping_cost"]   = df["shipping_cost"].astype(float).round(4)
    df["return_flag"]     = df["return_flag"].astype(bool)
    df["channel"]         = df["channel"].str.strip().str.lower()
    df["product_id"]      = df["product_id"].str.strip().str.upper()
    df["customer_id"]     = df["customer_id"].str.strip().str.upper()
    return df


def _derive_financial_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Re-derive amounts from source fields so downstream is always consistent."""
    df["gross_amount"]    = (df["quantity"] * df["unit_price"]).round(4)
    df["discount_amount"] = (df["gross_amount"] * df["discount_pct"]).round(4)
    df["net_amount"]      = (df["gross_amount"] - df["discount_amount"]).round(4)
    df["margin_amount"]   = (df["net_amount"] - (df["shipping_cost"])).round(4)
    return df


def _validate_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows that violate hard business invariants (log & drop)."""
    original = len(df)
    # net_amount must be non-negative
    bad_net = df["net_amount"] < 0
    if bad_net.any():
        logger.warning(f"[transform] Dropping {bad_net.sum()} rows with negative net_amount")
        df = df[~bad_net]
    # discount cannot exceed gross
    bad_disc = df["discount_amount"] > df["gross_amount"]
    if bad_disc.any():
        logger.warning(f"[transform] Dropping {bad_disc.sum()} rows where discount > gross")
        df = df[~bad_disc]
    if len(df) < original:
        logger.info(f"[transform] Rows after rule validation: {len(df):,} (dropped {original - len(df)})")
    return df.reset_index(drop=True)


def _resolve_product_sk(df: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    """Map product_id → product_sk using in-memory dim_product slice."""
    mapping = (
        products[products["is_active"] == True][["product_id", "product_sk"]]
        .drop_duplicates("product_id")
        .set_index("product_id")["product_sk"]
        .to_dict()
    )
    df["product_sk"] = df["product_id"].map(mapping)
    missing = df["product_sk"].isna().sum()
    if missing:
        logger.warning(f"[transform] {missing} rows have unmapped product_id → will be dropped")
        df = df.dropna(subset=["product_sk"])
    df["product_sk"] = df["product_sk"].astype(int)
    return df


def _resolve_customer_sk(df: pd.DataFrame, customers: pd.DataFrame) -> pd.DataFrame:
    """Map customer_id → customer_sk using in-memory dim_customer slice."""
    mapping = (
        customers[["customer_id", "customer_sk"]]
        .drop_duplicates("customer_id")
        .set_index("customer_id")["customer_sk"]
        .to_dict()
    )
    df["customer_sk"] = df["customer_id"].map(mapping)
    missing = df["customer_sk"].isna().sum()
    if missing:
        logger.warning(f"[transform] {missing} rows have unmapped customer_id → will be dropped")
        df = df.dropna(subset=["customer_sk"])
    df["customer_sk"] = df["customer_sk"].astype(int)
    return df


def _attach_date_key(df: pd.DataFrame) -> pd.DataFrame:
    """Derive integer date_key from sale_date (YYYYMMDD)."""
    df["date_key"] = pd.to_datetime(df["sale_date"]).dt.strftime("%Y%m%d").astype(int)
    return df


def _attach_pipeline_metadata(df: pd.DataFrame, run_id: Optional[UUID]) -> pd.DataFrame:
    df["pipeline_run_id"] = str(run_id) if run_id else "manual"
    df["loaded_at"]       = datetime.now(tz=timezone.utc)
    return df


def _compute_row_checksums(df: pd.DataFrame) -> pd.DataFrame:
    """SHA-256 of (sale_id, product_id, customer_id, sale_date, net_amount)."""
    key_cols = ["sale_id", "product_id", "customer_id", "net_amount"]
    def _hash(row: pd.Series) -> str:
        payload = "|".join(str(row[c]) for c in key_cols)
        return hashlib.sha256(payload.encode()).hexdigest()
    df["row_checksum"] = df.apply(_hash, axis=1)
    return df


# ---------------------------------------------------------------------------
# Aggregate helpers (used by validation tasks)
# ---------------------------------------------------------------------------

def compute_daily_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """Return a slim daily summary for validate_load gate."""
    return (
        df.groupby("sale_date")
          .agg(
            order_count   =("order_id",   "nunique"),
            line_items    =("sale_id",    "count"),
            total_units   =("quantity",   "sum"),
            total_gross   =("gross_amount","sum"),
            total_discount=("discount_amount","sum"),
            total_net     =("net_amount", "sum"),
          )
          .reset_index()
    )
