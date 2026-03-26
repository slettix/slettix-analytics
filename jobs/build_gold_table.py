"""
Build an aggregated Gold Delta table from a Silver source.

Reads the full Silver table, computes aggregations via Spark SQL,
then merges the result into Gold on the configured primary key.
This makes each run incremental: only changed aggregation rows are written.

Column and table descriptions from the config are stored as Delta
table/column properties so they are readable by any downstream tool.

Usage:
  spark-submit /opt/spark/jobs/build_gold_table.py \\
    --config /opt/spark/app/conf/gold/department_stats.json
"""

import argparse
import json
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format='{"ts": "%(asctime)s", "level": "%(levelname)s", "job": "build_gold_table", "msg": "%(message)s"}',
)
log = logging.getLogger(__name__)


def aggregate(spark: SparkSession, source: str) -> DataFrame:
    """Compute department-level metrics from Silver employees using Spark SQL."""
    spark.read.format("delta").load(source).createOrReplaceTempView("silver_employees")

    return spark.sql("""
        SELECT
            department,
            COUNT(*)          AS headcount,
            ROUND(AVG(salary), 2) AS avg_salary,
            MIN(salary)       AS min_salary,
            MAX(salary)       AS max_salary,
            SUM(salary)       AS total_payroll,
            current_timestamp() AS _gold_updated_at
        FROM silver_employees
        GROUP BY department
    """)


def upsert_to_gold(spark: SparkSession, df: DataFrame, target: str, primary_key: str) -> None:  # pragma: no cover
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, target):
        gold = DeltaTable.forPath(spark, target)
        (
            gold.alias("existing")
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
        log.info(f"Created Gold table at {target} with {df.count()} rows")


def apply_metadata(spark: SparkSession, target: str, config: dict) -> None:  # pragma: no cover
    """Store table description and column comments in Delta table properties."""
    table_ref = f"delta.`{target}`"

    if "description" in config:
        spark.sql(f"""
            ALTER TABLE {table_ref}
            SET TBLPROPERTIES ('comment' = '{config["description"]}')
        """)
        log.info("Set table description")

    for col_name, comment in config.get("columns", {}).items():
        safe_comment = comment.replace("'", "\\'")
        try:
            spark.sql(f"""
                ALTER TABLE {table_ref}
                ALTER COLUMN {col_name} COMMENT '{safe_comment}'
            """)
        except Exception as e:
            log.warning(f"Could not set comment for column '{col_name}': {e}")

    log.info("Applied column metadata")


def run(spark: SparkSession, config: dict) -> None:
    source      = config["source"]
    target      = config["target"]
    primary_key = config["primary_key"]

    log.info(f"Reading Silver from {source}")
    df = aggregate(spark, source)
    log.info(f"Aggregated {df.count()} department rows")

    upsert_to_gold(spark, df, target, primary_key)
    apply_metadata(spark, target, config)
    log.info("Done")


def main():
    parser = argparse.ArgumentParser(description="Build aggregated Gold Delta table")
    parser.add_argument("--config", required=True, help="Path to gold config JSON")
    args = parser.parse_args()

    config = json.loads(Path(args.config).read_text())

    spark = (
        SparkSession.builder
        .appName(f"build_gold:{config['target'].split('/')[-1]}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    run(spark, config)
    spark.stop()


if __name__ == "__main__":
    main()
