"""
Unit tests for transform_bronze_to_silver.py

Tests cover:
- apply_casts(): type coercion and null-on-failure behaviour
- apply_null_rules(): drop_if_null and fill
- drop_bronze_metadata(): removes Bronze-only columns
- add_silver_metadata(): adds _silver_updated_at
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType

sys.path.insert(0, str(Path(__file__).parent.parent / "jobs"))
from transform_bronze_to_silver import (  # noqa: E402
    add_silver_metadata,
    apply_casts,
    apply_null_rules,
    drop_bronze_metadata,
)


# ── Fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture
def employees_df(spark):
    """Bronze-style employees DataFrame with metadata columns."""
    data = [
        (1, "Alice",   "engineering", "95000",  "2022-03-01", "2024-01-15", "s3a://raw/employees"),
        (2, "Bob",     "marketing",   "72000",  "2021-07-15", "2024-01-15", "s3a://raw/employees"),
        (3, "Charlie", None,          "105000", "2020-01-10", "2024-01-15", "s3a://raw/employees"),
        (4, None,      "marketing",   "68000",  "2023-05-20", "2024-01-15", "s3a://raw/employees"),
        (5, "Eve",     "engineering", "not_a_number", "2019-11-03", "2024-01-15", "s3a://raw/employees"),
    ]
    return spark.createDataFrame(
        data,
        ["id", "name", "department", "salary", "hire_date", "ingestion_date", "_source_path"],
    )


# ── apply_casts ────────────────────────────────────────────────────────────


class TestApplyCasts:
    def test_casts_salary_to_integer(self, employees_df):
        df = apply_casts(employees_df, {"salary": "integer"})
        field = next(f for f in df.schema.fields if f.name == "salary")
        assert isinstance(field.dataType, IntegerType)

    def test_valid_salary_values_preserved(self, employees_df):
        df = apply_casts(employees_df, {"salary": "integer"})
        alice = df.filter(F.col("name") == "Alice").collect()[0]
        assert alice["salary"] == 95000

    def test_invalid_salary_becomes_null(self, employees_df):
        df = apply_casts(employees_df, {"salary": "integer"})
        eve = df.filter(F.col("name") == "Eve").collect()[0]
        assert eve["salary"] is None

    def test_missing_column_is_skipped(self, employees_df):
        # Should not raise, just log a warning
        df = apply_casts(employees_df, {"nonexistent_col": "integer"})
        assert df.count() == 5

    def test_row_count_unchanged_after_cast(self, employees_df):
        df = apply_casts(employees_df, {"salary": "integer"})
        assert df.count() == 5


# ── apply_null_rules ───────────────────────────────────────────────────────


class TestApplyNullRules:
    def test_drops_rows_with_null_in_required_col(self, employees_df):
        df = apply_null_rules(employees_df, {"drop_if_null": ["name"]})
        names = {r.name for r in df.select("name").collect()}
        assert None not in names

    def test_drop_removes_correct_count(self, employees_df):
        df = apply_null_rules(employees_df, {"drop_if_null": ["name"]})
        assert df.count() == 4  # row 4 (name=None) dropped

    def test_fill_replaces_null_department(self, employees_df):
        df = apply_null_rules(employees_df, {"fill": {"department": "unknown"}})
        charlie = df.filter(F.col("name") == "Charlie").collect()[0]
        assert charlie["department"] == "unknown"

    def test_fill_does_not_overwrite_existing_values(self, employees_df):
        df = apply_null_rules(employees_df, {"fill": {"department": "unknown"}})
        alice = df.filter(F.col("name") == "Alice").collect()[0]
        assert alice["department"] == "engineering"

    def test_empty_rules_leaves_df_unchanged(self, employees_df):
        df = apply_null_rules(employees_df, {})
        assert df.count() == 5


# ── drop_bronze_metadata ───────────────────────────────────────────────────


class TestDropBronzeMetadata:
    def test_removes_ingestion_date(self, employees_df):
        df = drop_bronze_metadata(employees_df)
        assert "ingestion_date" not in df.columns

    def test_removes_source_path(self, employees_df):
        df = drop_bronze_metadata(employees_df)
        assert "_source_path" not in df.columns

    def test_preserves_business_columns(self, employees_df):
        df = drop_bronze_metadata(employees_df)
        for col in ("id", "name", "department", "salary", "hire_date"):
            assert col in df.columns

    def test_row_count_unchanged(self, employees_df):
        df = drop_bronze_metadata(employees_df)
        assert df.count() == 5


# ── add_silver_metadata ────────────────────────────────────────────────────


class TestAddSilverMetadata:
    def test_adds_silver_updated_at_column(self, employees_df):
        df = add_silver_metadata(employees_df)
        assert "_silver_updated_at" in df.columns

    def test_silver_updated_at_is_timestamp(self, employees_df):
        df = add_silver_metadata(employees_df)
        field = next(f for f in df.schema.fields if f.name == "_silver_updated_at")
        assert isinstance(field.dataType, TimestampType)

    def test_no_nulls_in_silver_updated_at(self, employees_df):
        df = add_silver_metadata(employees_df)
        null_count = df.filter(F.col("_silver_updated_at").isNull()).count()
        assert null_count == 0
