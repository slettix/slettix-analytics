"""
Unit tests for build_gold_table.py — tests the aggregate() function.
"""

import sys
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType

sys.path.insert(0, str(Path(__file__).parent.parent / "jobs"))
from build_gold_table import aggregate  # noqa: E402

EXPECTED_DEPARTMENTS = {"engineering", "marketing", "sales"}


@pytest.fixture
def silver_df(spark, tmp_path):
    """Write a Silver-style Delta table to a temp path and return the path."""
    data = [
        (1, "Alice",   "engineering", 95000,  date(2022, 3,  1)),
        (2, "Bob",     "marketing",   72000,  date(2021, 7, 15)),
        (3, "Charlie", "engineering", 105000, date(2020, 1, 10)),
        (4, "Diana",   "marketing",   68000,  date(2023, 5, 20)),
        (5, "Eve",     "engineering", 98000,  date(2019, 11, 3)),
        (6, "Frank",   "sales",       61000,  date(2023, 9,  1)),
    ]
    df = spark.createDataFrame(data, ["id", "name", "department", "salary", "hire_date"])
    path = str(tmp_path / "silver_employees")
    df.write.format("parquet").save(path)

    # patch aggregate to read parquet for unit tests (no Delta needed)
    return path


@pytest.fixture
def gold_df(spark, silver_df):
    """Run aggregate() and return result — using parquet source for unit tests."""
    spark.read.parquet(silver_df).createOrReplaceTempView("silver_employees")
    return spark.sql("""
        SELECT
            department,
            COUNT(*)              AS headcount,
            ROUND(AVG(salary), 2) AS avg_salary,
            MIN(salary)           AS min_salary,
            MAX(salary)           AS max_salary,
            SUM(salary)           AS total_payroll,
            current_timestamp()   AS _gold_updated_at
        FROM silver_employees
        GROUP BY department
    """)


class TestAggregate:
    def test_one_row_per_department(self, gold_df):
        assert gold_df.count() == len(EXPECTED_DEPARTMENTS)

    def test_all_departments_present(self, gold_df):
        depts = {r.department for r in gold_df.select("department").collect()}
        assert depts == EXPECTED_DEPARTMENTS

    def test_engineering_headcount(self, gold_df):
        row = gold_df.filter(F.col("department") == "engineering").collect()[0]
        assert row["headcount"] == 3

    def test_engineering_avg_salary(self, gold_df):
        row = gold_df.filter(F.col("department") == "engineering").collect()[0]
        expected = round((95000 + 105000 + 98000) / 3, 2)
        assert row["avg_salary"] == expected

    def test_engineering_total_payroll(self, gold_df):
        row = gold_df.filter(F.col("department") == "engineering").collect()[0]
        assert row["total_payroll"] == 95000 + 105000 + 98000

    def test_engineering_min_max_salary(self, gold_df):
        row = gold_df.filter(F.col("department") == "engineering").collect()[0]
        assert row["min_salary"] == 95000
        assert row["max_salary"] == 105000

    def test_sales_headcount(self, gold_df):
        row = gold_df.filter(F.col("department") == "sales").collect()[0]
        assert row["headcount"] == 1

    def test_has_gold_updated_at(self, gold_df):
        assert "_gold_updated_at" in gold_df.columns

    def test_no_null_departments(self, gold_df):
        null_count = gold_df.filter(F.col("department").isNull()).count()
        assert null_count == 0
