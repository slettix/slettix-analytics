"""
Kafka → Bronze: Folkeregister personhendelser (IDP)

Leser hendelser fra fire Kafka-topics og lander dem som Delta-tabell i Bronze,
partisjonert på event_type og event_date.

Topics:
  - folkeregister.citizen.created   (citizen.created)
  - folkeregister.event.birth       (event.birth)
  - folkeregister.citizen.died      (citizen.died)
  - folkeregister.event.relocation  (event.relocation)

Kjør med spark-submit:
  spark-submit \\
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,\\
               io.delta:delta-spark_2.12:3.2.0,\\
               org.apache.hadoop:hadoop-aws:3.3.4,\\
               com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
    /opt/spark/jobs/folkeregister_person_idp.py \\
    [--bootstrap-servers host.docker.internal:9092] \\
    [--starting-offsets latest|earliest]

PII-tiltak:
  fnr (fødselsnummer) erstattes med SHA-256-hash (fnr_hash) umiddelbart i Bronze.
  Den originale fnr-kolonnen slettes og lagres aldri på disk.
  person_id (UUID) brukes som nøkkel videre nedstrøms.

Feiltoleranse:
  Checkpoint lagres i s3a://checkpoints/folkeregister/person_events.
  Jobben kan restartes uten å miste eller duplisere hendelser.
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


log = get_logger("folkeregister_person_idp")

TOPICS          = "folkeregister.citizen.created,folkeregister.event.birth,folkeregister.citizen.died,folkeregister.event.relocation"
TARGET_PATH     = "s3a://bronze/folkeregister/person_events"
CHECKPOINT_PATH = "s3a://checkpoints/folkeregister/person_events"
CONSUMER_GROUP  = "slettix-folkeregister-person-idp"
TRIGGER_SECONDS = 5

# CloudEvent-konvolutt-schema (camelCase — serialisert av C# JsonNamingPolicy.CamelCase).
# payload er en union av alle mulige felttyper på tvers av topics; felt som ikke
# finnes i en gitt payload-type vil være NULL etter parsing.
CLOUD_EVENT_SCHEMA = StructType([
    StructField("eventId",    StringType(), nullable=True),
    StructField("eventType",  StringType(), nullable=True),
    StructField("occurredAt", StringType(), nullable=True),
    StructField("source",     StringType(), nullable=True),
    StructField("version",    StringType(), nullable=True),
    StructField("payload", StructType([
        # CitizenPayload — citizen.created, citizen.died
        StructField("citizenId",  StringType(), nullable=True),
        StructField("ssn",        StringType(), nullable=True),
        StructField("firstName",  StringType(), nullable=True),
        StructField("lastName",   StringType(), nullable=True),
        StructField("sex",        StringType(), nullable=True),
        StructField("birthDate",  StringType(), nullable=True),
        StructField("country",    StringType(), nullable=True),
        StructField("status",     StringType(), nullable=True),
        # Adresse ved registrering (citizen.created fra v2)
        StructField("address",    StringType(), nullable=True),
        StructField("postalCode", StringType(), nullable=True),
        StructField("city",       StringType(), nullable=True),
        # AddressChangedPayload — event.relocation
        StructField("newPostalCode", StringType(), nullable=True),
        StructField("newCity",       StringType(), nullable=True),
        # BirthPayload — event.birth
        StructField("childId",   StringType(), nullable=True),
        StructField("childSsn",  StringType(), nullable=True),
        StructField("parent1Id", StringType(), nullable=True),
        StructField("parent2Id", StringType(), nullable=True),
    ]), nullable=True),
])


def parse_args():
    p = argparse.ArgumentParser(description="Folkeregister — person events IDP (Kafka → Bronze)")
    p.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "host.docker.internal:9092"),
        help="Kafka bootstrap servers (kommaseparert). Standard: host.docker.internal:9092",
    )
    p.add_argument(
        "--starting-offsets",
        default="latest",
        help=(
            "Kafka startposisjon. 'latest' for produksjon, 'earliest' for full replay, "
            "eller JSON-streng med per-partisjon offset, f.eks. "
            '{\"topic\":{\"0\":100,\"1\":200}} (standard: latest)'
        ),
    )
    return p.parse_args()


def build_stream(spark: SparkSession, bootstrap_servers: str, starting_offsets: str):
    """
    Les råmeldinger fra Kafka, pakk ut CloudEvent-konvolutten og flat ut til
    kanoniske Bronze-kolonner.
    """
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", TOPICS)
        .option("startingOffsets", starting_offsets)
        .option("kafka.group.id", CONSUMER_GROUP)
        .option("failOnDataLoss", "false")
        .option("kafka.session.timeout.ms", "30000")
        .option("kafka.request.timeout.ms", "40000")
        .load()
    )

    parsed = raw_df.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(F.col("value").cast("string"), CLOUD_EVENT_SCHEMA).alias("env"),
    )

    return parsed.select(
        "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        F.col("env.eventId").alias("event_id"),
        F.col("env.eventType").alias("event_type"),
        F.col("env.occurredAt").alias("event_timestamp"),
        # person_id: citizenId for citizen-topics, childId for event.birth
        F.coalesce(F.col("env.payload.citizenId"), F.col("env.payload.childId")).alias("person_id"),
        # fnr: ssn for citizen-topics, childSsn for event.birth (SHA-256 hashes i enrich)
        F.coalesce(F.col("env.payload.ssn"), F.col("env.payload.childSsn")).alias("fnr"),
        F.col("env.payload.firstName").alias("first_name"),
        F.col("env.payload.lastName").alias("last_name"),
        F.col("env.payload.birthDate").alias("birth_date"),
        F.col("env.payload.sex").alias("gender"),
        # Foreldre (kun satt for event.birth)
        F.col("env.payload.parent1Id").alias("mother_id"),
        F.col("env.payload.parent2Id").alias("father_id"),
        # Dødsdato: utled fra occurredAt når eventType = citizen.died
        F.when(F.col("env.eventType") == "citizen.died", F.col("env.occurredAt"))
         .cast("string").alias("death_date"),
        # Adresse: citizen.created (v2: address/postalCode/city) eller event.relocation (newPostalCode/newCity)
        F.col("env.payload.address").alias("street_address"),
        F.coalesce(
            F.col("env.payload.newPostalCode"),
            F.col("env.payload.postalCode"),
        ).alias("municipality_code"),
        F.coalesce(
            F.col("env.payload.newCity"),
            F.col("env.payload.city"),
        ).alias("municipality_name"),
        F.lit(None).cast("string").alias("county"),
    )


def enrich(df, source_topics: str):
    """
    Berik med prosesseringsmetadata og masker PII.

    PII-tiltak:
      - fnr → SHA-256 hash lagres som fnr_hash
      - fnr-kolonne slettes (aldri skrevet til disk)
    """
    return (
        df
        .withColumn("fnr_hash", F.sha2(F.col("fnr"), 256))
        .drop("fnr")
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_source_topics", F.lit(source_topics))
        .withColumn("event_date", F.to_date(F.coalesce(
            F.to_timestamp(F.col("event_timestamp")),
            F.col("kafka_timestamp"),
        )))
    )


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("folkeregister_person_idp")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info("Job startet", extra={
        "event":            "job_start",
        "topics":           TOPICS,
        "target":           TARGET_PATH,
        "checkpoint":       CHECKPOINT_PATH,
        "bootstrap_servers": args.bootstrap_servers,
        "starting_offsets": args.starting_offsets,
    })

    stream_df  = build_stream(spark, args.bootstrap_servers, args.starting_offsets)
    enriched   = enrich(stream_df, TOPICS)

    query = (
        enriched.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("event_type", "event_date")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start(TARGET_PATH)
    )

    log.info("Stream aktiv", extra={
        "event":     "stream_active",
        "query_id":  str(query.id),
        "trigger_s": TRIGGER_SECONDS,
    })

    query.awaitTermination()


if __name__ == "__main__":
    main()
