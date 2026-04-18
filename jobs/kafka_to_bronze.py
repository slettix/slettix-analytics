"""
Kafka → Bronze: Generisk IDP (Input Data Product)

Leser CloudEvent-meldinger fra ett eller flere Kafka-topics og lander dem
som Delta-tabell i Bronze.

CloudEvent-konvolutten (eventId, eventType, occurredAt, source, version) pakkes ut
til egne kolonner. Payload lagres som rå JSON-streng (payload_json) slik at
Silver-jobber kan transformere domenespesifikke felt uten at denne generiske
jobben trenger å kjenne til meldingsskjemaet.

Kjøring:
  spark-submit kafka_to_bronze.py \\
    --topics mitt.domene.events,mitt.domene.updated \\
    --target-path s3a://bronze/domene/events \\
    --consumer-group domene-events-idp \\
    [--checkpoint-path s3a://checkpoints/domene/events] \\
    [--bootstrap-servers kafka:9092] \\
    [--starting-offsets latest|earliest] \\
    [--trigger-seconds 5] \\
    [--partition-by event_type,event_date]

Feiltoleranse:
  Checkpoint lagres til --checkpoint-path (standard: s3a://checkpoints/<consumer-group>).
  Jobben kan restartes uten å miste eller duplisere hendelser (exactly-once via Delta).

PII-tiltak:
  Jobben gjør ingen PII-masking — det er ansvar for Silver-laget (domeneteam).
  Sensitiv data bør maskes/hashe i transformasjonssteg nedstrøms.
"""

import argparse
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

try:
    from spark_logger import get_logger
except ImportError:
    import logging

    def get_logger(name):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        return logging.getLogger(name)


log = get_logger("kafka_to_bronze")

# CloudEvent-konvolutt-schema (camelCase — serialisert av .NET/Java JsonNamingPolicy.CamelCase).
# Payload lagres som rå streng (payload_json) — domenespesifikk parsing gjøres i Silver.
_CLOUD_EVENT_SCHEMA = StructType([
    StructField("eventId",    StringType(), nullable=True),
    StructField("eventType",  StringType(), nullable=True),
    StructField("occurredAt", StringType(), nullable=True),
    StructField("source",     StringType(), nullable=True),
    StructField("version",    StringType(), nullable=True),
    StructField("payload",    StringType(), nullable=True),   # rå JSON — ikke parset
])


def parse_args():
    p = argparse.ArgumentParser(description="Generisk Kafka → Bronze IDP")
    p.add_argument(
        "--topics",
        required=True,
        help="Kommaseparerte Kafka-topics, f.eks. 'mitt.domene.created,mitt.domene.updated'",
    )
    p.add_argument(
        "--target-path",
        required=True,
        help="Mål-sti for Delta-tabell, f.eks. 's3a://bronze/domene/events'",
    )
    p.add_argument(
        "--consumer-group",
        required=True,
        help="Kafka consumer group ID, f.eks. 'domene-events-idp'",
    )
    p.add_argument(
        "--checkpoint-path",
        default=None,
        help="Checkpoint-sti (standard: s3a://checkpoints/<consumer-group>)",
    )
    p.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka.customermaster.svc.cluster.local:9092"),
        help="Kafka bootstrap-servere (kommaseparert)",
    )
    p.add_argument(
        "--starting-offsets",
        default="latest",
        help="Kafka startposisjon: 'latest', 'earliest' eller JSON-offset-map (standard: latest)",
    )
    p.add_argument(
        "--trigger-seconds",
        type=int,
        default=5,
        help="Micro-batch trigger-intervall i sekunder (standard: 5)",
    )
    p.add_argument(
        "--partition-by",
        default="event_type,event_date",
        help="Kommaseparerte kolonnenavn for Delta-partisjonering (standard: event_type,event_date)",
    )
    return p.parse_args()


def build_stream(spark: SparkSession, topics: str, bootstrap_servers: str,
                 starting_offsets: str, consumer_group: str):
    """
    Les råmeldinger fra Kafka og pakk ut CloudEvent-konvolutten.

    Payload lagres som payload_json (rå JSON) — Silver-jobber transformerer
    domenespesifikke felt uten at denne generiske jobben trenger å vite om dem.
    """
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topics)
        .option("startingOffsets", starting_offsets)
        .option("kafka.group.id", consumer_group)
        .option("failOnDataLoss", "false")
        .option("kafka.session.timeout.ms", "30000")
        .option("kafka.request.timeout.ms", "40000")
        .load()
    )

    # Forsøk å parse som CloudEvent; fall tilbake til rå verdi hvis parsing feiler
    parsed = raw_df.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("value").cast("string").alias("_raw_value"),
        F.from_json(
            F.col("value").cast("string"),
            _CLOUD_EVENT_SCHEMA,
        ).alias("env"),
    )

    return parsed.select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "_raw_value",
        F.col("env.eventId").alias("event_id"),
        F.col("env.eventType").alias("event_type"),
        F.col("env.occurredAt").alias("event_timestamp"),
        F.col("env.source").alias("event_source"),
        F.col("env.version").alias("event_version"),
        # Payload som rå JSON-streng — domeneteam parser i Silver
        F.col("env.payload").alias("payload_json"),
    )


def enrich(df, topics: str, consumer_group: str):
    """
    Legg til prosesseringsmetadata og partition-kolonner.
    """
    return (
        df
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_source_topics", F.lit(topics))
        .withColumn("_consumer_group", F.lit(consumer_group))
        .withColumn("event_date", F.to_date(F.coalesce(
            F.to_timestamp(F.col("event_timestamp")),
            F.col("kafka_timestamp"),
        )))
    )


def main():
    args = parse_args()

    checkpoint = args.checkpoint_path or f"s3a://checkpoints/{args.consumer_group}"
    partitions = [c.strip() for c in args.partition_by.split(",") if c.strip()]

    spark = (
        SparkSession.builder
        .appName(f"kafka_to_bronze:{args.consumer_group}")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info("Job startet", extra={
        "event":             "job_start",
        "topics":            args.topics,
        "target":            args.target_path,
        "checkpoint":        checkpoint,
        "consumer_group":    args.consumer_group,
        "bootstrap_servers": args.bootstrap_servers,
        "starting_offsets":  args.starting_offsets,
        "partition_by":      partitions,
    })

    stream_df = build_stream(
        spark,
        topics=args.topics,
        bootstrap_servers=args.bootstrap_servers,
        starting_offsets=args.starting_offsets,
        consumer_group=args.consumer_group,
    )
    enriched = enrich(stream_df, args.topics, args.consumer_group)

    writer = (
        enriched.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
    )
    if partitions:
        writer = writer.partitionBy(*partitions)

    query = writer.start(args.target_path)

    log.info("Stream aktiv", extra={
        "event":     "stream_active",
        "query_id":  str(query.id),
        "trigger_s": args.trigger_seconds,
    })

    query.awaitTermination()


if __name__ == "__main__":
    main()
