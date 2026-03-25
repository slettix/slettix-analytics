import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession for unit tests — no cluster or Delta needed."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("slettix-analytics-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
