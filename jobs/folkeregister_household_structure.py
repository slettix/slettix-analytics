"""
Silver → Gold: Familiestørrelsesfordeling og husstandstyper  (C-5)

Leser person_registry (gjeldende) og family_relations og klassifiserer
hver levende person inn i en husstandstype.

Husstandstyper:
  EKTEPAR_MED_BARN       — gift + minst ett barn (FORELDER_AV depth=1)
  EKTEPAR_UTEN_BARN      — gift + ingen barn
  ENSLIG_FORSØRGER       — ikke gift + minst ett barn
  ENSLIG                 — ikke gift + ingen barn

Størelseskategorier (family_size = partner + barn, maks per husstand):
  1 — enslig (ingen partner, ingen barn)
  2 — par eller enslig forsørger med 1 barn
  3–4, 5+ — større familier

Tabellen telles per (husstandstype, size_category): count og pct.

Kjøring:
  spark-submit /opt/spark/jobs/folkeregister_household_structure.py
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


log = get_logger("folkeregister_household_structure")

PERSON_REGISTRY_PATH  = "s3a://silver/folkeregister/person_registry"
FAMILY_RELATIONS_PATH = "s3a://silver/folkeregister/family_relations"
TARGET_PATH           = "s3a://gold/folkeregister/household_structure"


def build_household_structure(spark: SparkSession) -> None:

    log.info("Leser Silver person_registry (is_current, is_alive) …")
    persons = (
        spark.read.format("delta").load(PERSON_REGISTRY_PATH)
        .filter((F.col("is_current") == True) & (F.col("is_alive") == True))
        .select("person_id")
    )

    log.info("Leser Silver family_relations …")
    relations = spark.read.format("delta").load(FAMILY_RELATIONS_PATH)

    # ── Finn nåværende ektefeller ─────────────────────────────────────────
    partners = (
        relations
        .filter((F.col("relation_type") == "GIFT") & (F.col("is_current") == True))
        .select(F.col("person_id_a").alias("person_id"))
        .distinct()
    )

    # ── Finn foreldre med barn (direkte, generation_depth=1) ──────────────
    children_count = (
        relations
        .filter(
            (F.col("relation_type") == "FORELDER_AV") &
            (F.col("generation_depth") == 1)
        )
        .groupBy(F.col("person_id_a").alias("person_id"))
        .agg(F.count("*").alias("child_count"))
    )

    # ── Klassifiser hver levende person ──────────────────────────────────
    classified = (
        persons
        .join(partners.withColumn("has_partner", F.lit(True)),
              on="person_id", how="left")
        .join(children_count, on="person_id", how="left")
        .fillna(False, subset=["has_partner"])
        .fillna(0,     subset=["child_count"])
        .withColumn(
            "household_type",
            F.when(F.col("has_partner") & (F.col("child_count") > 0),
                   F.lit("EKTEPAR_MED_BARN"))
             .when(F.col("has_partner") & (F.col("child_count") == 0),
                   F.lit("EKTEPAR_UTEN_BARN"))
             .when(~F.col("has_partner") & (F.col("child_count") > 0),
                   F.lit("ENSLIG_FORSØRGER"))
             .otherwise(F.lit("ENSLIG"))
        )
        # family_size = 1 (selv) + partner (0/1) + barn
        .withColumn(
            "family_size",
            1 + F.when(F.col("has_partner"), F.lit(1)).otherwise(F.lit(0)) + F.col("child_count"),
        )
        .withColumn(
            "size_category",
            F.when(F.col("family_size") == 1, F.lit("1"))
             .when(F.col("family_size") == 2, F.lit("2"))
             .when(F.col("family_size") <= 4, F.lit("3-4"))
             .otherwise(F.lit("5+"))
        )
    )

    # ── Aggreger per (husstandstype, størelseskategori) ───────────────────
    agg = (
        classified
        .groupBy("household_type", "size_category")
        .agg(F.count("person_id").alias("count"))
    )

    total = classified.count()
    gold_df = (
        agg
        .withColumn("pct_of_total",
                    F.round(F.col("count") / F.lit(total) * 100, 2))
        .withColumn("_updated_at", F.current_timestamp())
        .orderBy("household_type", "size_category")
    )

    count = gold_df.count()
    log.info(f"Skriver {count:,} husstandsrader til {TARGET_PATH} …")

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
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
        .appName("folkeregister_household_structure")
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

    build_household_structure(spark)
    spark.stop()


if __name__ == "__main__":
    main()
