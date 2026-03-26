"""
Unit tests for stream_to_bronze.py

Tests the schema definition and the enrich() transformation.
Streaming I/O is not tested here (requires a running Spark cluster with S3A).
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, TimestampType

sys.path.insert(0, str(Path(__file__).parent.parent / "jobs"))
from stream_to_bronze import EVENT_SCHEMA, SOURCE_PATH, enrich  # noqa: E402


@pytest.fixture
def sample_df(spark):
    """Batch DataFrame that mimics what the stream source would produce."""
    return spark.createDataFrame(
        [
            ("id-001", "employee_hired",  201, "engineering", 88000, None),
            ("id-002", "salary_updated",   1,  "engineering", 102000, None),
            ("id-003", "employee_left",    6,  "sales",       None,   None),
        ],
        schema=EVENT_SCHEMA,  # explicit schema avoids type inference errors on all-None columns
    )


class TestEventSchema:
    def test_has_required_fields(self):
        field_names = {f.name for f in EVENT_SCHEMA.fields}
        assert {"event_id", "event_type", "employee_id", "department", "salary", "event_timestamp"}.issubset(field_names)

    def test_event_id_is_string(self):
        field = next(f for f in EVENT_SCHEMA.fields if f.name == "event_id")
        assert isinstance(field.dataType, StringType)

    def test_employee_id_is_integer(self):
        field = next(f for f in EVENT_SCHEMA.fields if f.name == "employee_id")
        assert isinstance(field.dataType, IntegerType)

    def test_event_timestamp_is_timestamp(self):
        field = next(f for f in EVENT_SCHEMA.fields if f.name == "event_timestamp")
        assert isinstance(field.dataType, TimestampType)

    def test_event_id_not_nullable(self):
        field = next(f for f in EVENT_SCHEMA.fields if f.name == "event_id")
        assert field.nullable is False


class TestEnrich:
    def test_adds_processed_at_column(self, sample_df):
        df = enrich(sample_df)
        assert "_processed_at" in df.columns

    def test_processed_at_is_timestamp(self, sample_df):
        df = enrich(sample_df)
        field = next(f for f in df.schema.fields if f.name == "_processed_at")
        assert isinstance(field.dataType, TimestampType)

    def test_adds_source_path_column(self, sample_df):
        df = enrich(sample_df)
        assert "_source_path" in df.columns

    def test_source_path_value(self, sample_df):
        df = enrich(sample_df)
        paths = {r._source_path for r in df.select("_source_path").collect()}
        assert paths == {SOURCE_PATH}

    def test_row_count_unchanged(self, sample_df):
        df = enrich(sample_df)
        assert df.count() == 3

    def test_original_columns_preserved(self, sample_df):
        df = enrich(sample_df)
        for col in ("event_id", "event_type", "employee_id", "department", "salary"):
            assert col in df.columns

    def test_null_salary_preserved(self, sample_df):
        df = enrich(sample_df)
        row = df.filter(F.col("event_type") == "employee_left").collect()[0]
        assert row["salary"] is None
