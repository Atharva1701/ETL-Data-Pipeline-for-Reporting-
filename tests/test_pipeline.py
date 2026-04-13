"""
tests/test_pipeline.py
----------------------
Unit and integration tests for the ETL pipeline.
Run with:  pytest tests/ -v
"""

import os
import sys
import pytest
import pandas as pd
import numpy as np
from datetime import datetime

# Make src importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extract   import extract, EXPECTED_COLUMNS
from src.transform import (
    transform,
    _normalise_column_names,
    _remove_duplicates,
    _clean_text_columns,
    _parse_dates,
    _cast_numerics,
    _handle_missing_values,
    _validate_values,
    _add_derived_columns,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def raw_csv_path():
    """Path to the generated raw CSV."""
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "raw", "sales_data.csv"
    )
    if not os.path.exists(path):
        pytest.skip("Raw CSV not generated yet. Run generate_data.py first.")
    return path


@pytest.fixture
def minimal_raw_df():
    """A minimal raw DataFrame with known properties for unit testing."""
    return pd.DataFrame({
        "order_id":        ["ORD-001", "ORD-002", "ORD-003", "ORD-001"],  # ORD-001 duplicated
        "customer_id":     ["CUST-1",  "CUST-2",  "CUST-3",  "CUST-1"],
        "customer_name":   ["  alice smith  ", "BOB JONES", None, "  alice smith  "],
        "category":        ["electronics", "CLOTHING", "  Books  ", "electronics"],
        "sub_category":    ["Laptops", "Footwear", "Fiction", "Laptops"],
        "unit_price":      ["299.99", "-50.00", "19.95", "299.99"],
        "quantity":        ["2", "3", None, "2"],
        "discount_pct":    ["10", None, "0", "10"],
        "payment_method":  ["Credit Card", "paypal", None, "Credit Card"],
        "order_date":      ["2023-05-15", "06/20/2023", "15-Mar-2023", "2023-05-15"],
        "shipping_status": ["delivered", "SHIPPED", "Processing", "delivered"],
        "region":          ["North", "South", "East", "North"],
        "state":           ["New York", "Texas", "Ohio", "New York"],
    })


@pytest.fixture
def clean_df(minimal_raw_df):
    """Full-pipeline clean DataFrame derived from minimal_raw_df."""
    return transform(minimal_raw_df)


# ─────────────────────────────────────────────────────────────────────────────
# Extract tests
# ─────────────────────────────────────────────────────────────────────────────

class TestExtract:

    def test_extract_returns_dataframe(self, raw_csv_path):
        df = extract(raw_csv_path)
        assert isinstance(df, pd.DataFrame)

    def test_extract_row_count(self, raw_csv_path):
        df = extract(raw_csv_path)
        assert len(df) >= 15_000, "Expected at least 15,000 rows"

    def test_extract_expected_columns(self, raw_csv_path):
        df = extract(raw_csv_path)
        cols = set(df.columns.str.strip().str.lower())
        assert EXPECTED_COLUMNS.issubset(cols)

    def test_extract_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            extract(str(tmp_path / "nonexistent.csv"))

    def test_extract_empty_file(self, tmp_path):
        empty = tmp_path / "empty.csv"
        empty.write_text("")
        with pytest.raises(ValueError):
            extract(str(empty))

    def test_extract_missing_columns(self, tmp_path):
        bad = tmp_path / "bad.csv"
        bad.write_text("col_a,col_b\n1,2\n")
        with pytest.raises(ValueError, match="missing columns"):
            extract(str(bad))


# ─────────────────────────────────────────────────────────────────────────────
# Transform – unit tests
# ─────────────────────────────────────────────────────────────────────────────

class TestNormaliseColumnNames:

    def test_lowercases_columns(self):
        df = pd.DataFrame({"OrderID": [1], "  Customer Name  ": ["x"]})
        result = _normalise_column_names(df)
        assert list(result.columns) == ["orderid", "customer name"]


class TestRemoveDuplicates:

    def test_removes_exact_dupes(self, minimal_raw_df):
        before = len(minimal_raw_df)
        result = _remove_duplicates(minimal_raw_df.copy())
        assert len(result) == before - 1

    def test_no_dupes_unchanged(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = _remove_duplicates(df)
        assert len(result) == 3


class TestCleanTextColumns:

    def test_strips_whitespace(self, minimal_raw_df):
        df = _normalise_column_names(minimal_raw_df.copy())
        result = _clean_text_columns(df)
        assert result["customer_name"].str.strip().eq(result["customer_name"]).all()

    def test_title_case_applied(self, minimal_raw_df):
        df = _normalise_column_names(minimal_raw_df.copy())
        result = _clean_text_columns(df)
        # "electronics" should become "Electronics"
        assert "Electronics" in result["category"].values


class TestParseDates:

    def test_iso_format(self):
        df = pd.DataFrame({"order_date": ["2023-01-15"]})
        result = _parse_dates(df)
        assert result["order_date"].iloc[0] == pd.Timestamp("2023-01-15")

    def test_us_format(self):
        df = pd.DataFrame({"order_date": ["06/20/2023"]})
        result = _parse_dates(df)
        assert result["order_date"].iloc[0] == pd.Timestamp("2023-06-20")

    def test_mon_format(self):
        df = pd.DataFrame({"order_date": ["15-Mar-2023"]})
        result = _parse_dates(df)
        assert result["order_date"].iloc[0] == pd.Timestamp("2023-03-15")

    def test_invalid_date_becomes_nat(self):
        df = pd.DataFrame({"order_date": ["not-a-date"]})
        result = _parse_dates(df)
        assert pd.isna(result["order_date"].iloc[0])


class TestCastNumerics:

    def test_unit_price_is_float(self, minimal_raw_df):
        df = _normalise_column_names(minimal_raw_df.copy())
        result = _cast_numerics(df)
        assert result["unit_price"].dtype == np.float64

    def test_non_numeric_becomes_nan(self):
        df = pd.DataFrame({
            "unit_price": ["not_a_number"],
            "quantity": ["abc"],
            "discount_pct": ["x"]
        })
        result = _cast_numerics(df)
        assert pd.isna(result["unit_price"].iloc[0])


class TestHandleMissingValues:

    def test_quantity_null_filled_with_median(self):
        df = pd.DataFrame({
            "order_id":    ["ORD-1", "ORD-2", "ORD-3"],
            "order_date":  pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            "unit_price":  [10.0, 20.0, 30.0],
            "quantity":    pd.array([2, pd.NA, 4], dtype="Int64"),
            "discount_pct": [0.0, 5.0, 0.0],
            "payment_method":  ["Card", None, "Card"],
            "shipping_status": ["Delivered", "Shipped", None],
            "customer_name":   ["Alice", None, "Bob"],
        })
        result = _handle_missing_values(df)
        assert result["quantity"].isna().sum() == 0

    def test_rows_without_order_id_dropped(self):
        df = pd.DataFrame({
            "order_id":   [None, "ORD-2"],
            "order_date": pd.to_datetime(["2023-01-01", "2023-01-02"]),
            "unit_price": [10.0, 20.0],
            "quantity":   pd.array([1, 2], dtype="Int64"),
            "discount_pct": [0.0, 0.0],
        })
        result = _handle_missing_values(df)
        assert len(result) == 1
        assert result.iloc[0]["order_id"] == "ORD-2"


class TestValidateValues:

    def test_negative_price_becomes_positive(self):
        df = pd.DataFrame({
            "unit_price":      [-50.0, 100.0],
            "quantity":        pd.array([1, 2], dtype="Int64"),
            "discount_pct":    [0.0, 5.0],
            "category":        ["Electronics", "Books"],
        })
        result = _validate_values(df)
        assert (result["unit_price"] > 0).all()

    def test_discount_clamped_to_100(self):
        df = pd.DataFrame({
            "unit_price":   [100.0],
            "quantity":     pd.array([1], dtype="Int64"),
            "discount_pct": [150.0],
            "category":     ["Books"],
        })
        result = _validate_values(df)
        assert result["discount_pct"].iloc[0] == 100.0

    def test_quantity_below_one_corrected(self):
        df = pd.DataFrame({
            "unit_price":   [50.0],
            "quantity":     pd.array([0], dtype="Int64"),
            "discount_pct": [0.0],
            "category":     ["Books"],
        })
        result = _validate_values(df)
        assert result["quantity"].iloc[0] >= 1


class TestAddDerivedColumns:

    def test_line_total_calculated(self):
        df = pd.DataFrame({
            "unit_price":   [100.0],
            "quantity":     pd.array([3], dtype="Int64"),
            "discount_pct": [10.0],
            "order_date":   pd.to_datetime(["2023-06-01"]),
            "shipping_status": ["Delivered"],
        })
        result = _add_derived_columns(df)
        # 100 * 3 = 300; discount = 30; line_total = 270
        assert result["discount_amount"].iloc[0] == pytest.approx(30.0)
        assert result["line_total"].iloc[0] == pytest.approx(270.0)

    def test_is_returned_flag(self):
        df = pd.DataFrame({
            "unit_price":      [50.0, 50.0],
            "quantity":        pd.array([1, 1], dtype="Int64"),
            "discount_pct":    [0.0, 0.0],
            "order_date":      pd.to_datetime(["2023-01-01", "2023-01-02"]),
            "shipping_status": ["Returned", "Delivered"],
        })
        result = _add_derived_columns(df)
        assert result["is_returned"].iloc[0] == True
        assert result["is_returned"].iloc[1] == False

    def test_order_year_month_format(self):
        df = pd.DataFrame({
            "unit_price":      [50.0],
            "quantity":        pd.array([1], dtype="Int64"),
            "discount_pct":    [0.0],
            "order_date":      pd.to_datetime(["2023-09-15"]),
            "shipping_status": ["Delivered"],
        })
        result = _add_derived_columns(df)
        assert result["order_year_month"].iloc[0] == "2023-09"


# ─────────────────────────────────────────────────────────────────────────────
# Full pipeline integration test
# ─────────────────────────────────────────────────────────────────────────────

class TestFullTransformPipeline:

    def test_output_has_derived_columns(self, clean_df):
        for col in ["line_total", "discount_amount", "order_year_month", "is_returned"]:
            assert col in clean_df.columns, f"Missing derived column: {col}"

    def test_no_duplicate_order_ids(self, clean_df):
        assert clean_df["order_id"].duplicated().sum() == 0

    def test_no_negative_prices(self, clean_df):
        assert (clean_df["unit_price"] >= 0).all()

    def test_order_date_is_datetime(self, clean_df):
        assert pd.api.types.is_datetime64_any_dtype(clean_df["order_date"])

    def test_category_title_case(self, clean_df):
        # All categories should be Title Case (no all-caps or all-lower)
        for val in clean_df["category"].dropna():
            assert val == val.title() or val in ("Other",), \
                f"Category not title-cased: {val!r}"

    def test_row_count_reduced_by_duplicates(self, minimal_raw_df, clean_df):
        # minimal_raw_df has 4 rows with 1 duplicate → expect 3 clean rows
        assert len(clean_df) == 3

    def test_full_pipeline_on_real_data(self, raw_csv_path):
        raw   = extract(raw_csv_path)
        clean = transform(raw)
        assert len(clean) > 14_000
        assert clean["line_total"].notna().all()
        assert clean["order_date"].notna().all()
