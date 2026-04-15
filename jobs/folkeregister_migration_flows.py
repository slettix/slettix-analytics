"""
Silver → Gold: Migrasjonsstrømmer mellom kommunepar  (C-4)

Leser residence_history og bygger en retningsbestemt flytmatrise over
kommuneparvise migrasjonerstrømmer per år.

Logikk:
  For hvert bostedsskifte (person har moved_out fra kommune A og
  moved_in til kommune B) registreres en flyt A→B.
  Netto-flyten (inn minus ut) er inkludert for hvert par og år.

Internt brukes Window-funksjon lag() over person_id, sortert på
moved_in_date, for å finne forrige bostedskommune.

Kolonner:
  from_municipality_code / name / county — avreisekommune
  to_municipality_code   / name / county — ankomstkommune
  reference_year                         — år for innflyttingen
  flow_count                             — antall personer som gjorde denne flyten
  return_flow_count                      — antall som gjorde flyten i motsatt retning
  net_flow                               — flow_count − return_flow_count

Kjøring:
  spark-submit /opt/spark/jobs/folkeregister_migration_flows.py
"""

import argparse
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

try:
    from spark_logger import get_logger
except ImportError:
    import logging

    def get_logger(name):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)


log = get_logger("folkeregister_migration_flows")

RESIDENCE_HISTORY_PATH = "s3a://silver/folkeregister/residence_history"
TARGET_PATH            = "s3a://gold/folkeregister/migration_flows"


def build_migration_flows(spark: SparkSession) -> None:

    log.info("Leser Silver residence_history …")
    residence = spark.read.format("delta").load(RESIDENCE_HISTORY_PATH)

    # ── Finn forrige kommune per person via lag() ─────────────────────────
    w = Window.partitionBy("person_id").orderBy("moved_in_date")

    moves = (
        residence
        .withColumn("from_municipality_code",
                    F.lag("municipality_code").over(w))
        .withColumn("from_municipality_name",
                    F.lag("municipality_name").over(w))
        .withColumn("from_county",
                    F.lag("county").over(w))
        # Behold bare faktiske bytter (første opphold har from = NULL)
        .filter(F.col("from_municipality_code").isNotNull())
        # Og skip ikke-bytter (person bor i samme kommune)
        .filter(F.col("from_municipality_code") != F.col("municipality_code"))
        .withColumn("reference_year", F.year("moved_in_date"))
        .filter(F.col("reference_year").isNotNull())
        .select(
            "from_municipality_code", "from_municipality_name", "from_county",
            F.col("municipality_code").alias("to_municipality_code"),
            F.col("municipality_name").alias("to_municipality_name"),
            F.col("county").alias("to_county"),
            "reference_year",
        )
    )

    # ── Aggreger flyten A→B per år ────────────────────────────────────────
    forward_flows = (
        moves
        .groupBy(
            "from_municipality_code", "from_municipality_name", "from_county",
            "to_municipality_code",   "to_municipality_name",   "to_county",
            "reference_year",
        )
        .agg(F.count("*").alias("flow_count"))
    )

    # ── Beregn returflyt B→A for netto ───────────────────────────────────
    return_flows = (
        forward_flows
        .select(
            F.col("to_municipality_code").alias("from_municipality_code"),
            F.col("from_municipality_code").alias("to_municipality_code"),
            "reference_year",
            F.col("flow_count").alias("return_flow_count"),
        )
    )

    gold_df = (
        forward_flows
        .join(
            return_flows,
            on=["from_municipality_code", "to_municipality_code", "reference_year"],
            how="left",
        )
        .fillna(0, subset=["return_flow_count"])
        .withColumn("net_flow",
                    F.col("flow_count") - F.col("return_flow_count"))
        .withColumn("_updated_at", F.current_timestamp())
    )

    count = gold_df.count()
    log.info(f"Skriver {count:,} flyt-rader til {TARGET_PATH} …")

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("reference_year")
        .save(TARGET_PATH)
    )
    log.info("Done.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--minio-endpoint",   default=os.getenv("MINIO_ENDPOINT",    "http://minio.slettix-analytics.svc.cluster.local:9000"))
    parser.add_argument("--minio-access-key", default=os.getenv("AWS_ACCESS_KEY_ID", "admin"))
    parser.add_argument("--minio-secret-key", default=os.getenv("AWS_SECRET_ACCESS_KEY", "changeme"))
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("folkeregister_migration_flows")
        .config("spark.sql.extensions",        "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint",               args.minio_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    build_migration_flows(spark)
    spark.stop()


if __name__ == "__main__":
    main()
