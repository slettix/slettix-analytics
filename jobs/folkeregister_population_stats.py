"""
Silver → Gold: Befolkningsstatistikk per kommune og år  (C-1)

Leser person_registry SCD2 og bygger en aggregert statistikktabell
per kommune per år — uten PII.

Logikk:
  For hvert år i datasettet identifiseres hvilken rad som var aktiv
  ved årslutt (31. desember) for hver person.  Dette gir ett bilde
  per (person_id, år) som aggregeres opp til kommunenivå.

Kolonner:
  municipality_code / name / county — kommuneidentifikasjon
  reference_year                    — statistikkår
  population_total                  — bosatte personer ved årslutt
  population_male / female          — fordelt på kjønn
  births                            — fødte i referanseåret
  deaths                            — døde i referanseåret
  avg_age                           — gjennomsnittsalder ved årslutt

Kjøring:
  spark-submit /opt/spark/jobs/folkeregister_population_stats.py
"""

import argparse
import os
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

try:
    from spark_logger import get_logger
except ImportError:
    import logging

    def get_logger(name):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)


log = get_logger("folkeregister_population_stats")

PERSON_REGISTRY_PATH = "s3a://silver/folkeregister/person_registry"
TARGET_PATH          = "s3a://gold/folkeregister/population_stats"


def build_population_stats(spark: SparkSession) -> None:

    log.info("Leser Silver person_registry …")
    registry = spark.read.format("delta").load(PERSON_REGISTRY_PATH)

    current_year = datetime.now().year

    # ── Generer årsrekke fra første gyldige rad til inneværende år ────────
    min_year_row = registry.select(F.min(F.year("valid_from"))).first()
    min_year = int(min_year_row[0]) if min_year_row[0] else 2000
    years_df = spark.range(min_year, current_year + 1).toDF("reference_year")

    # ── Kryss-join: en (person_id, year)-kombinasjon per rad ─────────────
    # Behold bare raden som var aktiv ved årslutt (31. des) for hvert år
    registry_with_year = registry.crossJoin(years_df).filter(
        (F.year("valid_from") <= F.col("reference_year")) &
        (
            F.col("valid_to").isNull() |
            (F.year("valid_to") > F.col("reference_year"))
        )
    )

    # Prioriter is_current=true ved like år (kan skje ved same-year records)
    w_dedup = Window.partitionBy("person_id", "reference_year").orderBy(
        F.col("is_current").desc(), F.col("valid_from").desc()
    )
    snapshot = (
        registry_with_year
        .withColumn("_rn", F.row_number().over(w_dedup))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── Befolkningsstatistikk per kommune og år ───────────────────────────
    pop_stats = (
        snapshot
        .filter(F.col("is_alive") == True)
        .filter(F.col("municipality_code").isNotNull())
        .groupBy("municipality_code", "municipality_name", "county", "reference_year")
        .agg(
            F.count("person_id").alias("population_total"),
            F.sum(F.when(F.col("gender") == "M", 1).otherwise(0)).alias("population_male"),
            F.sum(F.when(F.col("gender") == "F", 1).otherwise(0)).alias("population_female"),
            F.round(
                F.avg(F.col("reference_year") - F.year("birth_date")), 1
            ).alias("avg_age"),
        )
    )

    # ── Fødsler og dødsfall per år ─────────────────────────────────────────
    births = (
        registry
        .filter(F.col("change_event_type").isin("event.birth", "citizen.created"))
        .select("person_id", "municipality_code", F.year("valid_from").alias("reference_year"))
        .groupBy("municipality_code", "reference_year")
        .agg(F.count("person_id").alias("births"))
    )

    deaths = (
        registry
        .filter(F.col("death_date").isNotNull())
        .select("person_id", "municipality_code", F.year("death_date").alias("reference_year"))
        .groupBy("municipality_code", "reference_year")
        .agg(F.count("person_id").alias("deaths"))
    )

    gold_df = (
        pop_stats
        .join(births,  on=["municipality_code", "reference_year"], how="left")
        .join(deaths,  on=["municipality_code", "reference_year"], how="left")
        .fillna(0, subset=["births", "deaths"])
        .withColumn("_updated_at", F.current_timestamp())
    )

    count = gold_df.count()
    log.info(f"Skriver {count:,} rader til {TARGET_PATH} …")

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
        .appName("folkeregister_population_stats")
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

    build_population_stats(spark)
    spark.stop()


if __name__ == "__main__":
    main()
