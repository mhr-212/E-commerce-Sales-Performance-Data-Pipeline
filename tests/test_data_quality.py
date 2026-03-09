"""
Data quality tests — validate financial, referential, and completeness invariants
on the transformed DataFrame before it is loaded.
"""
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from etl.transform import (
    _attach_date_key,
    _derive_financial_metrics,
    _normalise_dtypes,
    _compute_row_checksums,
)
from etl.utils.metrics import dataframe_checksum, assert_count_variance


# ---------------------------------------------------------------------------
# Financial invariants
# ---------------------------------------------------------------------------

class TestFinancialInvariants:
    def test_net_amount_never_negative(self, sample_df):
        df = _normalise_dtypes(_derive_financial_metrics(sample_df.copy()))
        assert (df["net_amount"] >= 0).all(), "Found negative net_amounts"

    def test_gross_minus_discount_equals_net(self, sample_df):
        df = _normalise_dtypes(_derive_financial_metrics(sample_df.copy()))
        computed_net = (df["gross_amount"] - df["discount_amount"]).round(4)
        assert (df["net_amount"] == computed_net).all()

    def test_discount_amount_lte_gross(self, sample_df):
        df = _normalise_dtypes(_derive_financial_metrics(sample_df.copy()))
        assert (df["discount_amount"] <= df["gross_amount"]).all()

    def test_gross_equals_quantity_times_price(self, sample_df):
        df = _normalise_dtypes(_derive_financial_metrics(sample_df.copy()))
        expected = (df["quantity"] * df["unit_price"]).round(4)
        diff = (df["gross_amount"] - expected).abs()
        assert (diff < 0.0001).all()

    def test_shipping_cost_non_negative(self, sample_df):
        df = _normalise_dtypes(sample_df.copy())
        assert (df["shipping_cost"] >= 0).all()


# ---------------------------------------------------------------------------
# Referential integrity
# ---------------------------------------------------------------------------

class TestReferentialIntegrity:
    def test_no_null_sale_ids(self, sample_df):
        assert sample_df["sale_id"].notna().all()

    def test_no_null_order_ids(self, sample_df):
        assert sample_df["order_id"].notna().all()

    def test_no_null_product_ids(self, sample_df):
        assert sample_df["product_id"].notna().all()

    def test_no_null_customer_ids(self, sample_df):
        assert sample_df["customer_id"].notna().all()

    def test_sale_ids_unique(self, sample_df):
        assert sample_df["sale_id"].nunique() == len(sample_df), "Duplicate sale_ids detected"

    def test_channel_values_valid(self, sample_df):
        valid_channels = {"online", "mobile_app", "marketplace", "in_store"}
        df = _normalise_dtypes(sample_df.copy())
        invalid = set(df["channel"].unique()) - valid_channels
        assert not invalid, f"Invalid channels found: {invalid}"


# ---------------------------------------------------------------------------
# Date completeness
# ---------------------------------------------------------------------------

class TestDateCompleteness:
    def test_date_key_matches_sale_date(self, sample_df):
        df = _normalise_dtypes(sample_df.copy())
        df = _attach_date_key(df)
        expected = pd.to_datetime(df["sale_date"]).dt.strftime("%Y%m%d").astype(int)
        assert (df["date_key"] == expected).all()

    def test_all_sale_dates_within_range(self, sample_df):
        df = _normalise_dtypes(sample_df.copy())
        min_date = pd.Timestamp("2019-01-01")
        max_date = pd.Timestamp("2025-12-31")
        dates = pd.to_datetime(df["sale_date"])
        assert (dates >= min_date).all(), "Found dates before 2019-01-01"
        assert (dates <= max_date).all(), "Found dates after 2025-12-31"


# ---------------------------------------------------------------------------
# Checksum integrity
# ---------------------------------------------------------------------------

class TestChecksumIntegrity:
    def test_checksum_reproducible_across_runs(self, sample_df):
        df1 = _normalise_dtypes(_derive_financial_metrics(sample_df.copy()))
        df2 = _normalise_dtypes(_derive_financial_metrics(sample_df.copy()))
        cs1 = dataframe_checksum(df1, ["sale_id", "net_amount"])
        cs2 = dataframe_checksum(df2, ["sale_id", "net_amount"])
        assert cs1 == cs2

    def test_checksum_changes_when_data_changes(self, sample_df):
        df1 = _normalise_dtypes(_derive_financial_metrics(sample_df.copy()))
        df2 = _normalise_dtypes(_derive_financial_metrics(sample_df.copy()))
        df2.loc[0, "net_amount"] = 99999.0
        cs1 = dataframe_checksum(df1, ["sale_id", "net_amount"])
        cs2 = dataframe_checksum(df2, ["sale_id", "net_amount"])
        assert cs1 != cs2

    def test_row_checksums_are_sha256(self, sample_df):
        df = _normalise_dtypes(_derive_financial_metrics(sample_df.copy()))
        df = _compute_row_checksums(df)
        # SHA-256 hex digest is always 64 chars
        assert (df["row_checksum"].str.len() == 64).all()


# ---------------------------------------------------------------------------
# Count variance guard
# ---------------------------------------------------------------------------

class TestCountVarianceGuard:
    def test_within_tolerance_passes(self):
        assert_count_variance(1000, 999, max_pct=0.05, label="test")  # 0.1% drop, fine

    def test_exceeds_tolerance_raises(self):
        with pytest.raises(ValueError, match="Row count variance"):
            assert_count_variance(1000, 800, max_pct=0.05, label="test")  # 20% drop

    def test_zero_source_skips_check(self):
        # Should not raise even with 0 source rows
        assert_count_variance(0, 100, max_pct=0.05, label="test")
