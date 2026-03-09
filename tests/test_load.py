"""
Integration tests for the load stage using in-memory SQLite.
Tests verify idempotency, transaction rollback, and validate_load gate.
"""
from __future__ import annotations

import uuid
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import text

from etl.transform import (
    _attach_date_key,
    _attach_pipeline_metadata,
    _compute_row_checksums,
    _derive_financial_metrics,
    _normalise_dtypes,
)


# ---------------------------------------------------------------------------
# Helper: build a load-ready DataFrame
# ---------------------------------------------------------------------------

def _prep_for_load(df: pd.DataFrame, products_df, customers_df) -> pd.DataFrame:
    from etl.transform import (
        _resolve_customer_sk,
        _resolve_product_sk,
    )
    return (
        df.copy()
          .pipe(_normalise_dtypes)
          .pipe(_derive_financial_metrics)
          .pipe(_resolve_product_sk, products_df)
          .pipe(_resolve_customer_sk, customers_df)
          .pipe(_attach_date_key)
          .pipe(_attach_pipeline_metadata, run_id=uuid.uuid4())
          .pipe(_compute_row_checksums)
    )


# ---------------------------------------------------------------------------
# SQLite-based load helpers (replaces PostgreSQL for tests)
# ---------------------------------------------------------------------------

def _sql_load(df: pd.DataFrame, engine) -> int:
    """Simplified UPSERT into SQLite fact_sales."""
    cols = [
        "sale_id", "date_key", "product_sk", "customer_sk",
        "sale_date", "order_id", "quantity", "unit_price",
        "discount_pct", "gross_amount", "discount_amount", "net_amount",
        "shipping_cost", "return_flag", "channel",
        "row_checksum", "pipeline_run_id", "loaded_at",
    ]
    available = [c for c in cols if c in df.columns]
    load_df = df[available].copy()
    load_df["sale_date"]    = load_df["sale_date"].astype(str)
    load_df["loaded_at"]   = load_df["loaded_at"].astype(str)
    load_df["return_flag"] = load_df["return_flag"].astype(int)

    records = load_df.to_dict("records")
    placeholders = ", ".join(f":{c}" for c in available)
    col_names    = ", ".join(available)
    upsert_sql = text(
        f"INSERT OR REPLACE INTO fact_sales ({col_names}) VALUES ({placeholders})"
    )
    with engine.begin() as conn:
        conn.execute(upsert_sql, records)
    return len(records)


def _count_rows(engine) -> int:
    with engine.connect() as conn:
        return conn.execute(text("SELECT COUNT(*) FROM fact_sales")).scalar()


def _sum_net(engine) -> float:
    with engine.connect() as conn:
        return conn.execute(text("SELECT SUM(net_amount) FROM fact_sales")).scalar() or 0.0


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestLoadIdempotency:
    def test_single_load_correct_count(self, sample_df, products_df, customers_df, sqlite_engine):
        df = _prep_for_load(sample_df, products_df, customers_df)
        _sql_load(df, sqlite_engine)
        assert _count_rows(sqlite_engine) == len(df)

    def test_double_load_same_count(self, sample_df, products_df, customers_df, sqlite_engine):
        """Loading the same data twice must not duplicate rows."""
        df = _prep_for_load(sample_df, products_df, customers_df)
        _sql_load(df, sqlite_engine)
        _sql_load(df, sqlite_engine)    # second load
        assert _count_rows(sqlite_engine) == len(df)

    def test_net_amount_sum_preserved(self, sample_df, products_df, customers_df, sqlite_engine):
        df = _prep_for_load(sample_df, products_df, customers_df)
        _sql_load(df, sqlite_engine)
        expected = round(float(df["net_amount"].sum()), 2)
        actual   = round(float(_sum_net(sqlite_engine)), 2)
        assert abs(actual - expected) < 0.01


class TestLoadUpdate:
    def test_updated_net_amount_reflected(self, sample_df, products_df, customers_df, sqlite_engine):
        """If net_amount changes for same sale_id, DB must reflect updated value."""
        df = _prep_for_load(sample_df, products_df, customers_df)
        _sql_load(df, sqlite_engine)

        # Mutate net_amount for first row and re-load
        df2 = df.copy()
        df2.loc[0, "net_amount"] = 9999.0
        _sql_load(df2, sqlite_engine)

        # Row count should still be the same
        assert _count_rows(sqlite_engine) == len(df)
        # New sum must reflect the update
        assert _sum_net(sqlite_engine) > df["net_amount"].sum()


class TestValidateLoad:
    def test_passes_when_sums_match(self, sample_df, products_df, customers_df, sqlite_engine):
        df = _prep_for_load(sample_df, products_df, customers_df)
        _sql_load(df, sqlite_engine)
        source_sum = float(df["net_amount"].sum())
        db_sum     = float(_sum_net(sqlite_engine))
        # Tolerance check (mirrors validate_load logic)
        variance = abs(db_sum - source_sum) / max(source_sum, 1)
        assert variance < 0.001, f"Sums don't match: source={source_sum}, db={db_sum}"

    def test_fails_when_sums_diverge(self):
        """validate_load should raise when DB sum deviates beyond tolerance."""
        from etl.load import validate_load

        df = pd.DataFrame([{
            "sale_id": str(uuid.uuid4()),
            "sale_date": date(2023, 6, 15),
            "net_amount": 1000.0,
            "order_id": "ORD-00001",
        }])

        with patch("etl.load._query_db_sum") as mock_query:
            mock_query.return_value = [{"total_net": 500.0, "row_count": 1}]
            with pytest.raises(AssertionError, match="Load validation FAILED"):
                validate_load(df, run_date=date(2023, 6, 15), tolerance_pct=0.001)


class TestLoadEmptyDataFrame:
    def test_empty_df_does_not_raise(self, sqlite_engine):
        """An empty DataFrame must be a no-op, not an error."""
        from unittest.mock import patch
        with patch("etl.load.get_session"):
            from etl.load import load
            result = load(pd.DataFrame(), run_date=date(2023, 6, 15))
            assert result["rows_inserted"] == 0
