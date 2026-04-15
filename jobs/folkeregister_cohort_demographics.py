"""
Silver → Gold: Demografiprofil per kohort  (C-2)

Leser person_registry (gjeldende tilstand) og residence_history og
bygger en kohorttabell gruppert på fødselsdekade og kjønn.

Kohorter er definert av fødselsdekade (1950-tallet, 1960-tallet …).
Tabellen er aggregert — ingen PII.

Kolonner:
  birth_decade          — fødselsdekade (f.eks. 1970 = født 1970–1979)
  gender                — kjønn (M / F / X / ukjent)
  cohort_size           — antall personer i kohorten
  alive_count           — antall i live (per kjøredato)
  survival_rate_pct     — alive_count / cohort_size × 100
  married_count         — antall gift (marital_status = GIFT)
  married_rate_pct      — andel gift i kohorten
  divorced_count        — antall skilt
  avg_municipality_changes — gjennomsnittlig antall kommunebytter per person

Kjøring:
  spark-submit /opt/spark/jobs/folkeregister_cohort_demographics.py
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from spark_logger import get_logger
except ImportError:
    import logging

    def get_logger(name):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)


log = get_logger("folkeregister_cohort_demographics")

PERSON_REGISTRY_PATH   = "s3a://silver/folkeregister/person_registry"
RESIDENCE_HISTORY_PATH = "s3a://silver/folkeregister/residence_history"
TARGET_PATH            = "s3a://gold/folkeregister/cohort_demographics"


def build_cohort_demographics(spark: SparkSession) -> None:

    log.info("Leser Silver person_registry (is_current) …")
    current = (
        spark.read.format("delta").load(PERSON_REGISTRY_PATH)
        .filter(F.col("is_current") == True)
        .withColumn(
            "birth_decade",
            (F.floor(F.year("birth_date") / 10) * 10).cast("integer"),
        )
        .withColumn("gender", F.coalesce(F.col("gender"), F.lit("UKJENT")))
    )

    # ── Antall kommunebytter per person ───────────────────────────────────
    log.info("Leser Silver residence_history …")
    moves_per_person = (
        spark.read.format("delta").load(RESIDENCE_HISTORY_PATH)
        .groupBy("person_id")
        .agg((F.count("*") - 1).alias("municipality_changes"))
        # Antall opphold minus 1 = antall bytter (0 for de som aldri har flyttet)
    )

    current = current.join(moves_per_person, on="person_id", how="left")
    current = current.fillna(0, subset=["municipality_changes"])

    # ── Aggreger per kohort ───────────────────────────────────────────────
    gold_df = (
        current
        .filter(F.col("birth_decade").isNotNull())
        .groupBy("birth_decade", "gender")
        .agg(
            F.count("person_id").alias("cohort_size"),
            F.sum(F.when(F.col("is_alive") == True, 1).otherwise(0)).alias("alive_count"),
            F.sum(F.when(F.col("marital_status") == "GIFT", 1).otherwise(0)).alias("married_count"),
            F.sum(F.when(F.col("marital_status") == "SKILT", 1).otherwise(0)).alias("divorced_count"),
            F.round(F.avg("municipality_changes"), 2).alias("avg_municipality_changes"),
        )
        .withColumn(
            "survival_rate_pct",
            F.round(F.col("alive_count") / F.col("cohort_size") * 100, 1),
        )
        .withColumn(
            "married_rate_pct",
            F.round(F.col("married_count") / F.col("cohort_size") * 100, 1),
        )
        .withColumn("_updated_at", F.current_timestamp())
        .orderBy("birth_decade", "gender")
    )

    count = gold_df.count()
    log.info(f"Skriver {count:,} kohort-rader til {TARGET_PATH} …")

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("birth_decade")
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
        .appName("folkeregister_cohort_demographics")
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

    build_cohort_demographics(spark)
    spark.stop()


if __name__ == "__main__":
    main()
