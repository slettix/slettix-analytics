"""
Silver → Gold: Sivilstandsfordeling over tid og aldersgruppe  (C-3)

Leser person_registry SCD2-historikk og beregner sivilstandsfordeling
per år, aldersgruppe og sivilstatus.  Ingen PII — kun aggregerte tall.

Kolonne valid_from gir tidspunktet for hver tilstandsendring.
Alder beregnes som year(valid_from) - year(birth_date).

Aldersgrupper:
  0–17   — barn og unge
  18–29  — unge voksne
  30–44  — etableringsalder
  45–64  — midtlivsalder
  65+    — pensjonistalder

Kolonner:
  reference_year   — statistikkår (year(valid_from))
  age_group        — aldersgruppe (se over)
  marital_status   — UGIFT | GIFT | SKILT | ENKE_ENKEMANN
  count            — antall registrerte i denne kombinasjonen
  pct_of_age_group — andel av alle i samme år+aldersgruppe (%)

Kjøring:
  spark-submit /opt/spark/jobs/folkeregister_marital_status_trends.py
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


log = get_logger("folkeregister_marital_status_trends")

PERSON_REGISTRY_PATH = "s3a://silver/folkeregister/person_registry"
TARGET_PATH          = "s3a://gold/folkeregister/marital_status_trends"

AGE_GROUP_EXPR = (
    F.when(F.col("age") < 18,  F.lit("0-17"))
     .when(F.col("age") < 30,  F.lit("18-29"))
     .when(F.col("age") < 45,  F.lit("30-44"))
     .when(F.col("age") < 65,  F.lit("45-64"))
     .otherwise(F.lit("65+"))
)


def build_marital_status_trends(spark: SparkSession) -> None:

    log.info("Leser Silver person_registry (alle rader) …")
    registry = spark.read.format("delta").load(PERSON_REGISTRY_PATH)

    # ── Utled referanseår og alder for hvert SCD2-snapshot ───────────────
    enriched = (
        registry
        .withColumn("reference_year", F.year("valid_from"))
        .withColumn("age", F.col("reference_year") - F.year("birth_date"))
        .filter(F.col("age") >= 0)          # fjern rader uten birth_date
        .filter(F.col("reference_year").isNotNull())
        .withColumn("age_group", AGE_GROUP_EXPR)
        .withColumn("marital_status", F.coalesce(F.col("marital_status"), F.lit("UGIFT")))
    )

    # ── Aggreger per (år, aldersgruppe, sivilstatus) ──────────────────────
    agg = (
        enriched
        .groupBy("reference_year", "age_group", "marital_status")
        .agg(F.count("person_id").alias("count"))
    )

    # ── Beregn andel innenfor (år, aldersgruppe) ──────────────────────────
    w_pct = Window.partitionBy("reference_year", "age_group")
    gold_df = (
        agg
        .withColumn(
            "pct_of_age_group",
            F.round(F.col("count") / F.sum("count").over(w_pct) * 100, 1),
        )
        .withColumn("_updated_at", F.current_timestamp())
        .orderBy("reference_year", "age_group", "marital_status")
    )

    count = gold_df.count()
    log.info(f"Skriver {count:,} trend-rader til {TARGET_PATH} …")

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
        .appName("folkeregister_marital_status_trends")
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

    build_marital_status_trends(spark)
    spark.stop()


if __name__ == "__main__":
    main()
