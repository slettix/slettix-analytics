"""
helse_dar_dodsfall_enriched — Gold-jobb for dødsårsaksregisteret.

Bygger den denormaliserte fakta-tabellen `gold/helse/dar/dodsfall_enriched`:
  • Én rad per dødsfall (PK: dodsfall_id)
  • JSON-felter underliggendeArsak/eksternArsak parses til typede kolonner
  • ICD-10-koder joines mot helse.icd10 for å gi navn + kapittel inline

Kilder:
  • s3a://silver/helse/dar     (event-basert)
  • s3a://silver/helse/icd10   (kodeverk-referanse)

Mål:
  • s3a://gold/helse/dar/dodsfall_enriched

Kjøring:
  spark-submit /opt/spark/jobs/helse_dar_dodsfall_enriched.py
"""

import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_logger import get_logger

log = get_logger("helse_dar_dodsfall_enriched")

SILVER_DAR   = "s3a://silver/helse/dar"
SILVER_ICD10 = "s3a://silver/helse/icd10"
GOLD_TARGET  = "s3a://gold/helse/dar/dodsfall_enriched"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("helse_dar_dodsfall_enriched")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()

    dar = spark.read.format("delta").load(SILVER_DAR)
    icd = spark.read.format("delta").load(SILVER_ICD10)

    rows_in = dar.count()
    log.info(f"Lest {rows_in} dødsfalls-rader fra {SILVER_DAR}")

    # Parse JSON-felter fra Silver (de er lagret som strenger via payload_extract)
    parsed = dar.select(
        F.col("dodsfallId").alias("dodsfall_id"),
        F.col("personSyntId").alias("person_synt_id"),
        F.col("alder").cast("int").alias("alder"),
        F.col("kjonn").alias("kjonn"),
        F.col("kommune").alias("kommune_kode"),
        F.col("fylke").alias("fylke"),
        F.col("tidspunkt").alias("tidspunkt"),
        F.col("klassifisertTidspunkt").alias("klassifisert_tidspunkt"),
        F.col("icd10Versjon").alias("icd10_versjon"),
        F.get_json_object("underliggendeArsak", "$.icd10Kode").alias("u_kode"),
        F.get_json_object("eksternArsak",       "$.skadeKode").alias("e_skade_kode"),
        F.get_json_object("eksternArsak",       "$.skademekanismeKode").alias("e_meka_kode"),
    )

    # Reduserte ICD-10-views for join (lookup-tabell — broadcast for ytelse)
    icd_lookup = F.broadcast(
        icd.select(
            F.col("code"),
            F.col("name_no"),
            F.col("chapter_code"),
        )
    )

    # Tre venstre-joins mot ICD-10 (én per kode-kolonne)
    enriched = (
        parsed
        .join(icd_lookup.withColumnRenamed("code", "u_kode")
                        .withColumnRenamed("name_no", "underliggende_arsak_navn")
                        .withColumnRenamed("chapter_code", "underliggende_arsak_kapittel"),
              on="u_kode", how="left")
        .join(icd_lookup.withColumnRenamed("code", "e_skade_kode")
                        .withColumnRenamed("name_no", "ekstern_skade_navn")
                        .withColumnRenamed("chapter_code", "ekstern_skade_kapittel"),
              on="e_skade_kode", how="left")
        .join(icd_lookup.withColumnRenamed("code", "e_meka_kode")
                        .withColumnRenamed("name_no", "ekstern_skademekanisme_navn")
                        .withColumnRenamed("chapter_code", "ekstern_skademekanisme_kapittel"),
              on="e_meka_kode", how="left")
        .withColumnRenamed("u_kode", "underliggende_arsak_kode")
        .withColumnRenamed("e_skade_kode", "ekstern_skade_kode")
        .withColumnRenamed("e_meka_kode", "ekstern_skademekanisme_kode")
        .withColumn("_gold_updated_at", F.current_timestamp())
    )

    # Endelig kolonne-rekkefølge for tabellen
    enriched = enriched.select(
        "dodsfall_id", "person_synt_id",
        "alder", "kjonn", "kommune_kode", "fylke",
        "tidspunkt", "klassifisert_tidspunkt", "icd10_versjon",
        "underliggende_arsak_kode", "underliggende_arsak_navn", "underliggende_arsak_kapittel",
        "ekstern_skade_kode",         "ekstern_skade_navn",         "ekstern_skade_kapittel",
        "ekstern_skademekanisme_kode","ekstern_skademekanisme_navn","ekstern_skademekanisme_kapittel",
        "_gold_updated_at",
    )

    # Overskriv hele tabellen — Gold-aggregat regenereres komplett ved hver kjøring
    (
        enriched.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_TARGET)
    )

    rows_out = enriched.count()
    log.info(f"Skrev {rows_out} rader til {GOLD_TARGET} (elapsed {time.time()-t0:.1f}s)")

    spark.stop()


if __name__ == "__main__":
    main()
