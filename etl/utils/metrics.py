"""
Pipeline metrics: row counts, SHA-256 checksums, and audit log writing.
"""
from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone
from typing import Optional
from uuid import UUID

import pandas as pd

from etl.utils.db import get_session
from etl.utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Checksum helpers
# ---------------------------------------------------------------------------

def dataframe_checksum(df: pd.DataFrame, cols: list[str]) -> str:
    """
    Compute a deterministic SHA-256 over selected columns.
    Rows are sorted before hashing so the result is order-independent.
    """
    subset = df[cols].copy()
    subset = subset.sort_values(by=cols).reset_index(drop=True)
    csv_bytes = subset.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()


def row_checksum(row: dict) -> str:
    """SHA-256 over a single row's sorted key=value pairs."""
    canonical = "|".join(f"{k}={v}" for k, v in sorted(row.items()))
    return hashlib.sha256(canonical.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Variance guard — raises if count delta is suspicious
# ---------------------------------------------------------------------------

def assert_count_variance(
    source_count: int,
    target_count: int,
    max_pct: float = 0.05,
    label: str = "",
) -> None:
    """
    Raise ValueError if |target - source| / source > max_pct.
    Allows for legitimate filtering but catches runaway drops.
    """
    if source_count == 0:
        return
    delta = abs(target_count - source_count) / source_count
    logger.info(
        f"[metrics] {label} source={source_count} target={target_count} delta={delta:.2%}"
    )
    if delta > max_pct:
        raise ValueError(
            f"Row count variance {delta:.2%} exceeds threshold {max_pct:.2%} "
            f"for stage '{label}'"
        )


# ---------------------------------------------------------------------------
# Audit log writer
# ---------------------------------------------------------------------------

def write_audit(
    *,
    run_id: UUID,
    dag_id: str,
    task_id: str,
    run_date: date,
    stage: str,
    status: str,
    rows_processed: int = 0,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    rows_rejected: int = 0,
    source_checksum: Optional[str] = None,
    target_checksum: Optional[str] = None,
    error_message: Optional[str] = None,
    started_at: Optional[datetime] = None,
) -> None:
    """Insert a record into pipeline_audit_log."""
    now = datetime.now(tz=timezone.utc)
    record = {
        "run_id": str(run_id),
        "dag_id": dag_id,
        "task_id": task_id,
        "run_date": run_date,
        "stage": stage,
        "status": status,
        "rows_processed": rows_processed,
        "rows_inserted": rows_inserted,
        "rows_updated": rows_updated,
        "rows_rejected": rows_rejected,
        "source_checksum": source_checksum,
        "target_checksum": target_checksum,
        "error_message": error_message,
        "started_at": started_at or now,
        "finished_at": now,
    }
    try:
        with get_session() as session:
            from sqlalchemy import text
            session.execute(
                text(
                    """
                    INSERT INTO pipeline_audit_log
                        (run_id, dag_id, task_id, run_date, stage, status,
                         rows_processed, rows_inserted, rows_updated, rows_rejected,
                         source_checksum, target_checksum, error_message,
                         started_at, finished_at)
                    VALUES
                        (:run_id, :dag_id, :task_id, :run_date, :stage, :status,
                         :rows_processed, :rows_inserted, :rows_updated, :rows_rejected,
                         :source_checksum, :target_checksum, :error_message,
                         :started_at, :finished_at)
                    """
                ),
                record,
            )
        logger.info(f"[audit] Written: stage={stage} status={status} run_id={run_id}")
    except Exception as exc:
        # Audit failure must never crash the main pipeline
        logger.error(f"[audit] Failed to write audit log: {exc}")
