"""
Unit tests for ingest_to_bronze.py

Tests cover:
- read_source() for CSV, JSON and Parquet
- add_metadata() adds correct columns and values
- read_source() raises on unsupported format
"""

import sys
import os
from pathlib import Path
from datetime import date

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, TimestampType

# Allow importing from jobs/
sys.path.insert(0, str(Path(__file__).parent.parent / "jobs"))
from ingest_to_bronze import add_metadata, read_source  # noqa: E402

SAMPLE_DIR = Path(__file__).parent.parent / "data" / "sample"

EXPECTED_COLUMNS = {"id", "name", "department", "salary", "hire_date"}
EXPECTED_ROW_COUNT = 8


# ── read_source ────────────────────────────────────────────────────────────


class TestReadSource:
    def test_csv_reads_correct_row_count(self, spark, tmp_path):
        src = str(SAMPLE_DIR)
        df = read_source(spark, src, "csv")
        assert df.count() == EXPECTED_ROW_COUNT

    def test_csv_has_expected_columns(self, spark):
        df = read_source(spark, str(SAMPLE_DIR), "csv")
        assert EXPECTED_COLUMNS.issubset(set(df.columns))

    def test_json_reads_correct_row_count(self, spark):
        df = read_source(spark, str(SAMPLE_DIR), "json")
        assert df.count() == EXPECTED_ROW_COUNT

    def test_json_has_expected_columns(self, spark):
        df = read_source(spark, str(SAMPLE_DIR), "json")
        assert EXPECTED_COLUMNS.issubset(set(df.columns))

    def test_parquet_reads_correct_row_count(self, spark, tmp_path):
        # Generate a Parquet file from the CSV sample on the fly
        csv_df = read_source(spark, str(SAMPLE_DIR), "csv")
        parquet_path = str(tmp_path / "employees.parquet")
        csv_df.write.parquet(parquet_path)

        df = read_source(spark, str(tmp_path), "parquet")
        assert df.count() == EXPECTED_ROW_COUNT

    def test_parquet_has_expected_columns(self, spark, tmp_path):
        csv_df = read_source(spark, str(SAMPLE_DIR), "csv")
        parquet_path = str(tmp_path / "employees.parquet")
        csv_df.write.parquet(parquet_path)

        df = read_source(spark, str(tmp_path), "parquet")
        assert EXPECTED_COLUMNS.issubset(set(df.columns))

    def test_unsupported_format_raises(self, spark):
        with pytest.raises(ValueError, match="Unsupported format"):
            read_source(spark, str(SAMPLE_DIR), "xml")


# ── add_metadata ───────────────────────────────────────────────────────────


class TestAddMetadata:
    INGESTION_DATE = "2024-01-15"
    SOURCE_PATH = "s3a://raw/employees"

    @pytest.fixture
    def base_df(self, spark):
        return read_source(spark, str(SAMPLE_DIR), "csv")

    def test_adds_ingestion_date_column(self, base_df):
        df = add_metadata(base_df, self.INGESTION_DATE, self.SOURCE_PATH)
        assert "ingestion_date" in df.columns

    def test_ingestion_date_is_correct_value(self, base_df):
        df = add_metadata(base_df, self.INGESTION_DATE, self.SOURCE_PATH)
        dates = {row.ingestion_date for row in df.select("ingestion_date").collect()}
        assert dates == {date(2024, 1, 15)}

    def test_ingestion_date_is_date_type(self, base_df):
        df = add_metadata(base_df, self.INGESTION_DATE, self.SOURCE_PATH)
        field = next(f for f in df.schema.fields if f.name == "ingestion_date")
        assert isinstance(field.dataType, DateType)

    def test_adds_source_path_column(self, base_df):
        df = add_metadata(base_df, self.INGESTION_DATE, self.SOURCE_PATH)
        assert "_source_path" in df.columns

    def test_source_path_is_correct_value(self, base_df):
        df = add_metadata(base_df, self.INGESTION_DATE, self.SOURCE_PATH)
        paths = {row._source_path for row in df.select("_source_path").collect()}
        assert paths == {self.SOURCE_PATH}

    def test_adds_ingested_at_column(self, base_df):
        df = add_metadata(base_df, self.INGESTION_DATE, self.SOURCE_PATH)
        assert "_ingested_at" in df.columns

    def test_ingested_at_is_timestamp_type(self, base_df):
        df = add_metadata(base_df, self.INGESTION_DATE, self.SOURCE_PATH)
        field = next(f for f in df.schema.fields if f.name == "_ingested_at")
        assert isinstance(field.dataType, TimestampType)

    def test_row_count_unchanged_after_metadata(self, base_df):
        df = add_metadata(base_df, self.INGESTION_DATE, self.SOURCE_PATH)
        assert df.count() == EXPECTED_ROW_COUNT
