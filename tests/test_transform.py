"""
Unit tests for etl/transform.py

Tests cover:
  - net_amount derivation correctness (including edge cases)
  - dtype normalisation
  - business rule enforcement (negative amounts, discount > gross)
  - surrogate key resolution (happy path + unmapped keys)
  - date_key derivation
  - row checksum determinism
  - daily aggregate summary shape
"""
from __future__ import annotations

import hashlib
import uuid
from datetime import date

import pandas as pd
import pytest

from etl.transform import (
    _attach_date_key,
    _compute_row_checksums,
    _derive_financial_metrics,
    _normalise_dtypes,
    _resolve_customer_sk,
    _resolve_product_sk,
    _validate_business_rules,
    compute_daily_aggregates,
    transform,
)


# ---------------------------------------------------------------------------
# _normalise_dtypes
# ---------------------------------------------------------------------------

class TestNormaliseDtypes:
    def test_channel_lowercased(self, sample_df):
        sample_df.loc[0, "channel"] = "  ONLINE  "
        result = _normalise_dtypes(sample_df.copy())
        assert result.loc[0, "channel"] == "online"

    def test_product_id_uppercased(self, sample_df):
        sample_df.loc[0, "product_id"] = "prod-00001"
        result = _normalise_dtypes(sample_df.copy())
        assert result.loc[0, "product_id"] == "PROD-00001"

    def test_numeric_columns_rounded(self, sample_df):
        sample_df.loc[0, "unit_price"] = 99.999_99
        result = _normalise_dtypes(sample_df.copy())
        assert result.loc[0, "unit_price"] == round(99.999_99, 4)


# ---------------------------------------------------------------------------
# _derive_financial_metrics
# ---------------------------------------------------------------------------

class TestDeriveFinancialMetrics:
    def test_basic_calculation(self, sample_df):
        df = _normalise_dtypes(sample_df.copy())
        result = _derive_financial_metrics(df)
        row = result.iloc[0]
        expected_gross   = round(row["quantity"] * row["unit_price"], 4)
        expected_disc    = round(expected_gross * row["discount_pct"], 4)
        expected_net     = round(expected_gross - expected_disc, 4)
        assert row["gross_amount"]    == pytest.approx(expected_gross, abs=0.0001)
        assert row["discount_amount"] == pytest.approx(expected_disc,  abs=0.0001)
        assert row["net_amount"]      == pytest.approx(expected_net,   abs=0.0001)

    def test_zero_discount(self, sample_df):
        sample_df["discount_pct"] = 0.0
        df = _normalise_dtypes(sample_df.copy())
        result = _derive_financial_metrics(df)
        assert (result["discount_amount"] == 0).all()
        assert (result["net_amount"] == result["gross_amount"]).all()

    def test_full_discount(self, sample_df):
        """100% discount → net_amount == 0."""
        import numpy as np
        sample_df["discount_pct"] = 1.0
        df = _normalise_dtypes(sample_df.copy())
        result = _derive_financial_metrics(df)
        assert np.allclose(result["net_amount"], 0.0, atol=0.0001)

    def test_net_never_negative_with_valid_discount(self, sample_df):
        """Discount ≤ 1 must never produce negative net."""
        df = _normalise_dtypes(sample_df.copy())
        result = _derive_financial_metrics(df)
        assert (result["net_amount"] >= 0).all()


# ---------------------------------------------------------------------------
# _validate_business_rules
# ---------------------------------------------------------------------------

class TestValidateBusinessRules:
    def test_drops_negative_net(self, sample_df):
        df = _normalise_dtypes(_derive_financial_metrics(sample_df.copy()))
        df.loc[0, "net_amount"] = -1.00
        result = _validate_business_rules(df)
        assert len(result) == len(sample_df) - 1

    def test_drops_discount_exceeds_gross(self, sample_df):
        df = _normalise_dtypes(_derive_financial_metrics(sample_df.copy()))
        df.loc[1, "discount_amount"] = df.loc[1, "gross_amount"] + 10
        result = _validate_business_rules(df)
        assert len(result) == len(sample_df) - 1

    def test_valid_rows_untouched(self, sample_df):
        df = _normalise_dtypes(_derive_financial_metrics(sample_df.copy()))
        result = _validate_business_rules(df)
        assert len(result) == len(sample_df)


# ---------------------------------------------------------------------------
# Surrogate key resolution
# ---------------------------------------------------------------------------

class TestResolveSurrogateKeys:
    def test_product_sk_resolved(self, sample_df, products_df):
        df = _normalise_dtypes(sample_df.copy())
        result = _resolve_product_sk(df, products_df)
        assert "product_sk" in result.columns
        assert result["product_sk"].notna().all()

    def test_unmapped_product_dropped(self, sample_df, products_df):
        df = _normalise_dtypes(sample_df.copy())
        df.loc[0, "product_id"] = "PROD-UNKNOWN"
        result = _resolve_product_sk(df, products_df)
        assert len(result) == len(sample_df) - 1

    def test_customer_sk_resolved(self, sample_df, customers_df):
        df = _normalise_dtypes(sample_df.copy())
        result = _resolve_customer_sk(df, customers_df)
        assert "customer_sk" in result.columns
        assert result["customer_sk"].notna().all()


# ---------------------------------------------------------------------------
# _attach_date_key
# ---------------------------------------------------------------------------

class TestAttachDateKey:
    def test_date_key_format(self, sample_df):
        df = _normalise_dtypes(sample_df.copy())
        result = _attach_date_key(df)
        assert result["date_key"].iloc[0] == 20230615   # date(2023, 6, 15)

    def test_date_key_is_integer(self, sample_df):
        df = _normalise_dtypes(sample_df.copy())
        result = _attach_date_key(df)
        assert result["date_key"].dtype in ("int64", "int32")


# ---------------------------------------------------------------------------
# Row checksums
# ---------------------------------------------------------------------------

class TestRowChecksums:
    def test_checksum_deterministic(self, sample_df):
        df = _normalise_dtypes(sample_df.copy())
        r1 = _compute_row_checksums(df.copy())
        r2 = _compute_row_checksums(df.copy())
        assert r1["row_checksum"].equals(r2["row_checksum"])

    def test_different_rows_different_checksums(self, sample_df):
        df = _normalise_dtypes(sample_df.copy())
        result = _compute_row_checksums(df)
        # With 100 unique sale_ids the checksums should all be unique
        assert result["row_checksum"].nunique() == len(result)

    def test_checksum_length(self, sample_df):
        df = _normalise_dtypes(sample_df.copy())
        result = _compute_row_checksums(df)
        assert (result["row_checksum"].str.len() == 64).all()


# ---------------------------------------------------------------------------
# compute_daily_aggregates
# ---------------------------------------------------------------------------

class TestComputeDailyAggregates:
    def test_returns_expected_columns(self, sample_df):
        df = _normalise_dtypes(_derive_financial_metrics(sample_df.copy()))
        agg = compute_daily_aggregates(df)
        for col in ("sale_date", "order_count", "total_net", "total_units"):
            assert col in agg.columns

    def test_total_net_matches_source(self, sample_df):
        df = _normalise_dtypes(_derive_financial_metrics(sample_df.copy()))
        agg = compute_daily_aggregates(df)
        assert agg["total_net"].sum() == pytest.approx(df["net_amount"].sum(), rel=1e-5)


# ---------------------------------------------------------------------------
# transform() — integration smoke test
# ---------------------------------------------------------------------------

class TestTransformIntegration:
    def test_transform_returns_dataframe(self, sample_df, products_df, customers_df):
        result = transform(
            sample_df,
            run_date=date(2023, 6, 15),
            products_df=products_df,
            customers_df=customers_df,
        )
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_all_required_columns_present(self, sample_df, products_df, customers_df):
        result = transform(
            sample_df,
            run_date=date(2023, 6, 15),
            products_df=products_df,
            customers_df=customers_df,
        )
        for col in ("row_checksum", "date_key", "pipeline_run_id", "net_amount"):
            assert col in result.columns

    def test_transform_empty_df_returns_empty(self):
        result = transform(pd.DataFrame(), run_date=date(2023, 6, 15))
        assert result.empty
