"""
Ingest files from a source path in MinIO (raw/) to Bronze as Delta tables.
Supports CSV, JSON and Parquet as input formats.

Usage:
  spark-submit /opt/spark/jobs/ingest_to_bronze.py \\
    --source s3a://raw/employees \\
    --target s3a://bronze/employees \\
    --format csv \\
    --ingestion-date 2024-01-15        # optional, defaults to today

Idempotency:
  Uses Delta replaceWhere to overwrite only the ingestion_date partition
  being loaded. Re-running with the same date is safe.
"""

import argparse
import time
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_logger import get_logger

log = get_logger("ingest_to_bronze")

SUPPORTED_FORMATS = ("csv", "json", "parquet")


def read_source(spark: SparkSession, source: str, fmt: str) -> DataFrame:
    """Read all files of the given format from source path."""
    reader = spark.read.option("inferSchema", "true")

    if fmt == "csv":
        return reader.option("header", "true").csv(f"{source}/*.csv")
    elif fmt == "json":
        return reader.json(f"{source}/*.json")
    elif fmt == "parquet":
        return reader.parquet(f"{source}/*.parquet")

    raise ValueError(f"Unsupported format '{fmt}'. Choose from: {SUPPORTED_FORMATS}")


def add_metadata(df: DataFrame, ingestion_date: str, source: str) -> DataFrame:
    """Add standard metadata columns used across all Bronze tables."""
    return df.withColumns({
        "ingestion_date": F.lit(ingestion_date).cast("date"),
        "_source_path":   F.lit(source),
        "_ingested_at":   F.current_timestamp(),
    })


def write_bronze(df: DataFrame, target: str, ingestion_date: str) -> None:
    """Write DataFrame to a Delta table, replacing only the given date partition."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingestion_date = '{ingestion_date}'")
        .partitionBy("ingestion_date")
        .save(target)
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest files to Bronze Delta table")
    parser.add_argument("--source", required=True, help="Source path (e.g. s3a://raw/employees)")
    parser.add_argument("--target", required=True, help="Target Delta path (e.g. s3a://bronze/employees)")
    parser.add_argument("--format", dest="fmt", default="csv", choices=SUPPORTED_FORMATS,
                        help="Input file format (default: csv)")
    parser.add_argument("--ingestion-date", default=str(date.today()),
                        help="Ingestion date YYYY-MM-DD (default: today)")
    return parser.parse_args()


def main():
    args = parse_args()
    t0 = time.time()

    spark = (
        SparkSession.builder
        .appName(f"ingest_to_bronze:{args.target.split('/')[-1]}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info("Job started", extra={
        "event": "job_start", "source": args.source,
        "target": args.target, "ingestion_date": args.ingestion_date,
    })

    df = read_source(spark, args.source, args.fmt)
    row_count = df.count()
    log.info("Source read", extra={"event": "read_done", "rows_read": row_count, "source": args.source})

    df = add_metadata(df, args.ingestion_date, args.source)
    write_bronze(df, args.target, args.ingestion_date)

    log.info("Job completed", extra={
        "event": "job_end", "rows_written": row_count,
        "target": args.target, "elapsed_s": round(time.time() - t0, 2),
    })
    spark.stop()


if __name__ == "__main__":
    main()
