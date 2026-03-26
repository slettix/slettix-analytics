"""
Spark Structured Streaming: watch s3a://raw/stream-input/ for new JSON files
and append them to s3a://bronze/stream_events as a Delta table.

Checkpointing to s3a://checkpoints/stream_events ensures that the job can be
restarted without reprocessing already-handled files.

Usage:
  spark-submit /opt/spark/jobs/stream_to_bronze.py

Stop with Ctrl-C (or kill the process). The checkpoint guarantees exactly-once
delivery — restarting picks up exactly where it left off.

Test by uploading a JSON file to s3a://raw/stream-input/.
The event should appear in s3a://bronze/stream_events within ~10 seconds.

Event schema (JSON Lines, one object per file or per line):
  {
    "event_id":        "uuid",
    "event_type":      "employee_hired | salary_updated | employee_left",
    "employee_id":     123,
    "department":      "engineering",
    "salary":          95000,
    "event_timestamp": "2024-01-15T10:30:00"
  }
"""

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format='{"ts": "%(asctime)s", "level": "%(levelname)s", "job": "stream_to_bronze", "msg": "%(message)s"}',
)
log = logging.getLogger(__name__)

SOURCE_PATH     = "s3a://raw/stream-input"
TARGET_PATH     = "s3a://bronze/stream_events"
CHECKPOINT_PATH = "s3a://checkpoints/stream_events"
TRIGGER_SECONDS = 10

# Schema must be defined explicitly for streaming — no inferSchema
EVENT_SCHEMA = StructType([
    StructField("event_id",        StringType(),    nullable=False),
    StructField("event_type",      StringType(),    nullable=True),
    StructField("employee_id",     IntegerType(),   nullable=True),
    StructField("department",      StringType(),    nullable=True),
    StructField("salary",          IntegerType(),   nullable=True),
    StructField("event_timestamp", TimestampType(), nullable=True),
])


def build_stream(spark: SparkSession):
    """Return a streaming DataFrame that reads new JSON files from SOURCE_PATH."""
    return (
        spark.readStream
        .schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", 20)   # process at most 20 files per micro-batch
        .json(SOURCE_PATH)
    )


def enrich(df):
    """Add processing metadata before writing to Bronze."""
    return df.withColumns({
        "_processed_at": F.current_timestamp(),
        "_source_path":  F.lit(SOURCE_PATH),
    })


def main():
    spark = (
        SparkSession.builder
        .appName("stream_to_bronze")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info(f"Starting stream: {SOURCE_PATH} → {TARGET_PATH}")
    log.info(f"Checkpoint: {CHECKPOINT_PATH}")
    log.info(f"Trigger interval: {TRIGGER_SECONDS}s")

    stream_df = build_stream(spark)
    enriched_df = enrich(stream_df)

    query = (
        enriched_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start(TARGET_PATH)
    )

    log.info("Stream running. Upload JSON files to s3a://raw/stream-input/ to trigger processing.")
    log.info("Stop with Ctrl-C — checkpoint ensures no data loss on restart.")

    query.awaitTermination()


if __name__ == "__main__":
    main()
