"""
helse_dar_medvirkende_arsaker — Gold-jobb for bridge mellom dødsfall og
medvirkende ICD-10-årsaker.

Bygger M:N-bridge-tabellen `gold/helse/dar/medvirkende_arsaker`:
  • Én rad per (dodsfall_id, sekvens-i-array)
  • Eksploderer JSON-arrayen `medvirkendeArsaker` fra Silver
  • Joiner mot helse.icd10 for å gi standardisert navn + kapittel

Kilder:
  • s3a://silver/helse/dar     (medvirkendeArsaker-array)
  • s3a://silver/helse/icd10   (kodeverk-referanse)

Mål:
  • s3a://gold/helse/dar/medvirkende_arsaker

Kjøring:
  spark-submit /opt/spark/jobs/helse_dar_medvirkende_arsaker.py
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_logger import get_logger

log = get_logger("helse_dar_medvirkende_arsaker")

SILVER_DAR   = "s3a://silver/helse/dar"
SILVER_ICD10 = "s3a://silver/helse/icd10"
GOLD_TARGET  = "s3a://gold/helse/dar/medvirkende_arsaker"

# Schema for hvert element i medvirkendeArsaker-arrayen
MEDVIRKENDE_SCHEMA = T.ArrayType(T.StructType([
    T.StructField("sekvens",     T.IntegerType(), True),
    T.StructField("kapittel",    T.StringType(),  True),
    T.StructField("icd10Kode",   T.StringType(),  True),
    T.StructField("icd10Tittel", T.StringType(),  True),
]))


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("helse_dar_medvirkende_arsaker")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()

    dar = spark.read.format("delta").load(SILVER_DAR)
    icd = spark.read.format("delta").load(SILVER_ICD10)

    rows_in = dar.count()
    log.info(f"Lest {rows_in} dødsfalls-rader fra {SILVER_DAR}")

    # Parse JSON-array → struct-array → explode → en rad per medvirkende
    exploded = (
        dar
        .select(
            F.col("dodsfallId").alias("dodsfall_id"),
            F.from_json(F.col("medvirkendeArsaker"), MEDVIRKENDE_SCHEMA).alias("m"),
        )
        .where(F.col("m").isNotNull() & (F.size("m") > 0))
        .select(
            "dodsfall_id",
            F.explode("m").alias("item"),
        )
        .select(
            "dodsfall_id",
            F.col("item.sekvens").alias("sekvens"),
            F.col("item.icd10Kode").alias("icd10_kode"),
        )
    )

    icd_lookup = F.broadcast(
        icd.select(
            F.col("code").alias("icd10_kode"),
            F.col("name_no").alias("icd10_navn"),
            F.col("chapter_code").alias("icd10_kapittel"),
        )
    )

    enriched = (
        exploded
        .join(icd_lookup, on="icd10_kode", how="left")
        .select(
            "dodsfall_id",
            "sekvens",
            "icd10_kode",
            "icd10_navn",
            "icd10_kapittel",
        )
        .withColumn("_gold_updated_at", F.current_timestamp())
    )

    (
        enriched.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_TARGET)
    )

    rows_out = enriched.count()
    log.info(f"Skrev {rows_out} bridge-rader til {GOLD_TARGET} (elapsed {time.time()-t0:.1f}s)")

    spark.stop()


if __name__ == "__main__":
    main()
