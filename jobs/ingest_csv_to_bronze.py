"""
Ingest CSV files from a source path in MinIO (raw/) to Bronze as Delta tables.

Usage:
  spark-submit /opt/spark/jobs/ingest_csv_to_bronze.py \\
    --source s3a://raw/employees \\
    --target s3a://bronze/employees \\
    --ingestion-date 2024-01-15        # optional, defaults to today

Idempotency:
  Uses Delta replaceWhere to overwrite only the ingestion_date partition
  being loaded. Re-running with the same date is safe.
"""

import argparse
import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format='{"ts": "%(asctime)s", "level": "%(levelname)s", "job": "ingest_csv_to_bronze", "msg": "%(message)s"}',
)
log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest CSV files to Bronze Delta table")
    parser.add_argument("--source", required=True, help="Source path (e.g. s3a://raw/employees)")
    parser.add_argument("--target", required=True, help="Target Delta path (e.g. s3a://bronze/employees)")
    parser.add_argument(
        "--ingestion-date",
        default=str(date.today()),
        help="Ingestion date (YYYY-MM-DD). Used as partition value. Default: today",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName(f"ingest_csv_to_bronze:{args.target.split('/')[-1]}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info(f"Reading CSVs from {args.source}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{args.source}/*.csv")
    )

    row_count = df.count()
    log.info(f"Read {row_count} rows from source")

    # Add metadata columns
    df = df.withColumns({
        "ingestion_date": F.lit(args.ingestion_date).cast("date"),
        "_source_path":   F.lit(args.source),
        "_ingested_at":   F.current_timestamp(),
    })

    # Write to Delta — replaceWhere makes the job idempotent per date partition
    log.info(f"Writing {row_count} rows to {args.target} (partition: ingestion_date={args.ingestion_date})")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingestion_date = '{args.ingestion_date}'")
        .partitionBy("ingestion_date")
        .save(args.target)
    )

    log.info(f"Done — wrote {row_count} rows to {args.target}")
    spark.stop()


if __name__ == "__main__":
    main()
