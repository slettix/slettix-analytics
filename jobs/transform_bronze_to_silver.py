"""
Transform Bronze Delta table → Silver Delta table.

Applies null handling and type casting rules from a config file,
then upserts the result into Silver using Delta merge on the primary key.

Usage:
  spark-submit /opt/spark/jobs/transform_bronze_to_silver.py \\
    --config /opt/spark/conf/silver/employees.json

Config format (see conf/silver/employees.json):
  {
    "source":      "s3a://bronze/employees",
    "target":      "s3a://silver/employees",
    "primary_key": "id",
    "null_handling": {
      "drop_if_null": ["id", "name"],
      "fill":         {"department": "unknown", "salary": 0}
    },
    "cast": {
      "id": "integer",
      "salary": "integer",
      "hire_date": "date"
    }
  }
"""

import argparse
import json
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format='{"ts": "%(asctime)s", "level": "%(levelname)s", "job": "transform_bronze_to_silver", "msg": "%(message)s"}',
)
log = logging.getLogger(__name__)

# Bronze metadata columns that should not carry over to Silver
BRONZE_META_COLS = {"ingestion_date", "_source_path", "_ingested_at"}


def apply_casts(df: DataFrame, cast_rules: dict) -> DataFrame:
    """
    Cast columns using try_cast so bad values become null (rather than crashing).
    Logs the number of nulls introduced per column.
    """
    for col_name, target_type in cast_rules.items():
        if col_name not in df.columns:
            log.warning(f"Cast rule references missing column '{col_name}' — skipping")
            continue

        before_nulls = df.filter(F.col(col_name).isNull()).count()
        df = df.withColumn(col_name, F.col(col_name).cast(target_type))
        after_nulls = df.filter(F.col(col_name).isNull()).count()

        introduced = after_nulls - before_nulls
        if introduced > 0:
            log.warning(f"Cast '{col_name}' → {target_type}: {introduced} value(s) could not be cast and became null")
        else:
            log.info(f"Cast '{col_name}' → {target_type}: OK")

    return df


def apply_null_rules(df: DataFrame, null_handling: dict) -> DataFrame:
    """Apply drop and fill rules for null values."""
    drop_cols = null_handling.get("drop_if_null", [])
    fill_map  = null_handling.get("fill", {})

    if drop_cols:
        before = df.count()
        df = df.dropna(subset=drop_cols)
        dropped = before - df.count()
        if dropped:
            log.warning(f"Dropped {dropped} row(s) with nulls in: {drop_cols}")

    if fill_map:
        df = df.fillna(fill_map)
        log.info(f"Filled nulls: {fill_map}")

    return df


def drop_bronze_metadata(df: DataFrame) -> DataFrame:
    """Remove Bronze-specific metadata columns from Silver data."""
    cols_to_drop = [c for c in BRONZE_META_COLS if c in df.columns]
    return df.drop(*cols_to_drop)


def add_silver_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("_silver_updated_at", F.current_timestamp())


def upsert_to_silver(spark: SparkSession, df: DataFrame, target: str, primary_key: str) -> None:  # pragma: no cover
    """
    Merge df into the Silver Delta table on primary_key.
    - Matching rows are updated.
    - New rows are inserted.
    """
    from delta.tables import DeltaTable  # lazy — only needed at runtime with Delta JARs

    if DeltaTable.isDeltaTable(spark, target):
        silver = DeltaTable.forPath(spark, target)
        (
            silver.alias("existing")
            .merge(
                df.alias("incoming"),
                f"existing.{primary_key} = incoming.{primary_key}",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        log.info(f"Merged {df.count()} rows into {target}")
    else:
        df.write.format("delta").save(target)
        log.info(f"Created Silver table at {target} with {df.count()} rows")


def run(spark: SparkSession, config: dict) -> None:
    source      = config["source"]
    target      = config["target"]
    primary_key = config["primary_key"]
    null_rules  = config.get("null_handling", {})
    cast_rules  = config.get("cast", {})

    log.info(f"Reading Bronze from {source}")
    df = spark.read.format("delta").load(source)
    log.info(f"Read {df.count()} rows")

    df = apply_casts(df, cast_rules)
    df = apply_null_rules(df, null_rules)
    df = drop_bronze_metadata(df)
    df = add_silver_metadata(df)

    upsert_to_silver(spark, df, target, primary_key)
    log.info("Done")


def main():
    parser = argparse.ArgumentParser(description="Bronze → Silver transformation")
    parser.add_argument("--config", required=True, help="Path to silver config JSON")
    args = parser.parse_args()

    config = json.loads(Path(args.config).read_text())

    spark = (
        SparkSession.builder
        .appName(f"bronze_to_silver:{config['target'].split('/')[-1]}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    run(spark, config)
    spark.stop()


if __name__ == "__main__":
    main()
