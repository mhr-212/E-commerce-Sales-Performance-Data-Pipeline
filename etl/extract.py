"""
Extract stage: read chunked CSV files, validate schema with pandera,
route bad rows to dead-letter directory, return clean DataFrame.
"""
from __future__ import annotations

import glob
import hashlib
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import UUID

import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check

from etl.utils.logger import get_logger
from etl.utils.metrics import dataframe_checksum, write_audit

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Schema contract
# ---------------------------------------------------------------------------

SALES_SCHEMA = DataFrameSchema(
    {
        "sale_id":          Column(str,   nullable=False, unique=True),
        "order_id":         Column(str,   nullable=False),
        "sale_date":        Column(pa.DateTime, nullable=False,
                                  checks=Check(lambda s: s >= pd.Timestamp("2019-01-01"))),
        "product_id":       Column(str,   nullable=False),
        "customer_id":      Column(str,   nullable=False),
        "quantity":         Column(int,   nullable=False, checks=Check.gt(0)),
        "unit_price":       Column(float, nullable=False, checks=Check.gt(0)),
        "discount_pct":     Column(float, nullable=False,
                                  checks=Check.in_range(0.0, 1.0)),
        "gross_amount":     Column(float, nullable=False, checks=Check.ge(0)),
        "discount_amount":  Column(float, nullable=False, checks=Check.ge(0)),
        "net_amount":       Column(float, nullable=False, checks=Check.ge(0)),
        "shipping_cost":    Column(float, nullable=False, checks=Check.ge(0)),
        "return_flag":      Column(bool,  nullable=False),
        "channel":          Column(str,   nullable=False,
                                  checks=Check.isin(["online","mobile_app","marketplace","in_store"])),
    },
    coerce=True,
    drop_invalid_rows=False,
)

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract(
    run_date: date,
    data_dir: str = "data/raw",
    dead_letter_dir: str = "data/dead_letter",
    chunk_size: int = 100_000,
    run_id: Optional[UUID] = None,
    dag_id: str = "sales_etl",
    task_id: str = "extract",
) -> pd.DataFrame:
    """
    Read all CSVs matching *run_date* year/month from data_dir.
    Returns validated, clean DataFrame ready for transform().
    Bad rows are written to dead_letter_dir with error annotations.
    """
    started_at = datetime.now(tz=timezone.utc)
    pattern    = str(Path(data_dir) / f"sales_{run_date.year}_{run_date.month:02d}.csv")
    files      = sorted(glob.glob(pattern))

    if not files:
        logger.warning(f"[extract] No files found for pattern: {pattern}")
        _write_audit(run_id, dag_id, task_id, run_date, "extract",
                     "success", 0, 0, started_at=started_at)
        return pd.DataFrame()

    all_clean:  list[pd.DataFrame] = []
    all_bad:    list[pd.DataFrame] = []
    total_rows = 0

    for fpath in files:
        logger.info(f"[extract] Reading {fpath}")
        for chunk in pd.read_csv(fpath, chunksize=chunk_size, parse_dates=["sale_date"]):
            total_rows += len(chunk)
            clean, bad = _validate_chunk(chunk)
            all_clean.append(clean)
            if not bad.empty:
                all_bad.append(bad)

    clean_df = pd.concat(all_clean, ignore_index=True) if all_clean else pd.DataFrame()
    bad_df   = pd.concat(all_bad,   ignore_index=True) if all_bad   else pd.DataFrame()

    # Write dead-letter
    if not bad_df.empty:
        dl_path = Path(dead_letter_dir) / f"dead_{run_date.year}_{run_date.month:02d}_{run_id or 'unknown'}.csv"
        bad_df.to_csv(dl_path, index=False)
        logger.warning(f"[extract] {len(bad_df):,} bad rows → {dl_path}")

    checksum = dataframe_checksum(clean_df, ["sale_id", "net_amount"]) if not clean_df.empty else ""
    logger.info(f"[extract] Clean rows: {len(clean_df):,} | Rejected: {len(bad_df):,} | Checksum: {checksum[:12]}…")

    _write_audit(
        run_id, dag_id, task_id, run_date, "extract", "success",
        rows_processed=total_rows,
        rows_inserted=len(clean_df),
        rows_rejected=len(bad_df),
        source_checksum=checksum,
        started_at=started_at,
    )
    return clean_df


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _validate_chunk(chunk: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Use pandera lazy validation to separate valid vs invalid rows."""
    try:
        valid = SALES_SCHEMA.validate(chunk, lazy=True)
        return valid, pd.DataFrame()
    except pa.errors.SchemaErrors as exc:
        failure_cases = exc.failure_cases[["index", "check", "failure_case"]]
        bad_indices   = set(failure_cases["index"].dropna().astype(int))
        bad           = chunk.iloc[list(bad_indices)].copy()
        bad["_error"] = (
            failure_cases.groupby("index")["check"]
            .apply(lambda s: "; ".join(s.astype(str)))
            .reindex(list(bad_indices))
            .values
        )
        clean = chunk.drop(index=list(bad_indices)).reset_index(drop=True)
        clean = SALES_SCHEMA.validate(clean)   # should now pass
        return clean, bad


def _write_audit(run_id, dag_id, task_id, run_date, stage, status,
                 rows_processed=0, rows_inserted=0, rows_rejected=0,
                 source_checksum=None, started_at=None):
    if run_id is None:
        return
    try:
        write_audit(
            run_id=run_id, dag_id=dag_id, task_id=task_id,
            run_date=run_date, stage=stage, status=status,
            rows_processed=rows_processed, rows_inserted=rows_inserted,
            rows_rejected=rows_rejected,
            source_checksum=source_checksum,
            started_at=started_at,
        )
    except Exception as exc:
        logger.error(f"[extract] Could not write audit: {exc}")
