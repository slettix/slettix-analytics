"""
Bronze → Silver: Folkeregister personregister (SCD Type 2)

Leser alle personhendelser fra Bronze og bygger et autoritativt personregister
med fullstendig historikk via SCD Type 2.  Tabellen bevarer alle tilstandsendringer
for navn, adresse, vitalstatus og sivilstatus.

SCD2-triggere per person_id (sortert på event_ts):
  PersonRegistrert / PersonFødt   → oppretter initiell rad
  PersonFlyttet                   → ny rad med oppdatert adresse
  PersonDød                       → ny rad med is_alive=false, death_date satt
  EkteskapInngått / EkteskapOppløst → ny rad med oppdatert sivilstatus

PII-tiltak:
  Tabellen er «restricted».  Kun fnr_hash lagres — original fnr lagres aldri.

Kjøring:
  spark-submit /opt/spark/jobs/folkeregister_person_registry.py
"""

import argparse
import os
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

try:
    from spark_logger import get_logger
except ImportError:
    import logging

    def get_logger(name):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)


log = get_logger("folkeregister_person_registry")

PERSON_EVENTS_PATH = "s3a://bronze/folkeregister/person_events"
FAMILY_EVENTS_PATH = "s3a://bronze/folkeregister/family_events"
TARGET_PATH        = "s3a://silver/folkeregister/person_registry"

STATE_EVENTS = {"citizen.created", "event.birth", "citizen.died", "event.relocation"}


# ── Hjelpefunksjon: null-literal-kolonner ───────────────────────────────────

def _nulls(*cols: str):
    return [F.lit(None).cast("string").alias(c) for c in cols]


# ── Bygge en unified state-change event-tabell ─────────────────────────────

def _person_state_events(person_df: DataFrame) -> DataFrame:
    return person_df.filter(
        F.col("event_type").isin(*STATE_EVENTS)
    ).select(
        "person_id", "event_ts", "event_type",
        "fnr_hash", "first_name", "last_name",
        "birth_date", "gender", "mother_id", "father_id",
        "street_address", "municipality_code", "municipality_name", "county",
        "death_date",
        F.lit(None).cast("string").alias("civil_status_change"),
    )


def _civil_status_events(family_df: DataFrame) -> DataFrame:
    """Syntetiske hendelser for sivilstatusendringer fra familiehendelser."""
    _static = _nulls(
        "fnr_hash", "first_name", "last_name", "birth_date",
        "gender", "mother_id", "father_id",
        "street_address", "municipality_code", "municipality_name", "county", "death_date",
    )

    frames = []
    for event_type, person_col, status in [
        ("event.marriage", "person_id_a", "GIFT"),
        ("event.marriage", "person_id_b", "GIFT"),
        ("event.divorce",  "person_id_a", "SKILT"),
        ("event.divorce",  "person_id_b", "SKILT"),
    ]:
        frames.append(
            family_df.filter(F.col("event_type") == event_type).select(
                F.col(person_col).alias("person_id"),
                "event_ts",
                F.lit("SivilstatusEndret").alias("event_type"),
                *_static,
                F.lit(status).alias("civil_status_change"),
            )
        )

    return reduce(DataFrame.unionByName, frames)


# ── Hovedlogikk ─────────────────────────────────────────────────────────────

def build_registry(spark: SparkSession) -> None:

    # ── Les Bronze-tabeller ───────────────────────────────────────────────
    log.info("Leser Bronze person_events …")
    person_df = (
        spark.read.format("delta").load(PERSON_EVENTS_PATH)
        .withColumn("event_ts", F.coalesce(
            F.to_timestamp("event_timestamp"), F.col("kafka_timestamp"),
        ))
    )
    # Bakoverkompatibel: legg til street_address som null hvis kolonnen mangler
    # (Bronze-tabellen har gammelt skjema inntil IDP re-kjøres med ny kode)
    if "street_address" not in person_df.columns:
        person_df = person_df.withColumn("street_address", F.lit(None).cast("string"))

    try:
        log.info("Leser Bronze family_events …")
        family_df = (
            spark.read.format("delta").load(FAMILY_EVENTS_PATH)
            .withColumn("event_ts", F.coalesce(
                F.to_timestamp("event_timestamp"), F.col("kafka_timestamp"),
            ))
        )
        civil_df = _civil_status_events(family_df)
    except Exception as exc:
        log.warning(f"Fant ikke family_events ({exc}) — sivilstatus settes til UGIFT.")
        civil_df = None

    # ── Bygg unified event-timeline ───────────────────────────────────────
    timeline = _person_state_events(person_df)
    if civil_df is not None:
        timeline = timeline.unionByName(civil_df)

    # ── Akkumuler tilstand med unbounded preceding window ─────────────────
    w_acc = (
        Window.partitionBy("person_id")
        .orderBy("event_ts")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    for src, dst in [
        ("fnr_hash",           "fnr_hash"),
        ("first_name",         "first_name"),
        ("last_name",          "last_name"),
        ("birth_date",         "birth_date"),
        ("gender",             "gender"),
        ("mother_id",          "mother_id"),
        ("father_id",          "father_id"),
        ("street_address",     "street_address"),
        ("municipality_code",  "municipality_code"),
        ("municipality_name",  "municipality_name"),
        ("county",             "county"),
        ("death_date",         "death_date"),
        ("civil_status_change","civil_status"),
    ]:
        timeline = timeline.withColumn(
            f"_s_{dst}",
            F.last(src, ignorenulls=True).over(w_acc),
        )

    timeline = timeline.withColumn("_s_civil_status",
        F.coalesce(F.col("_s_civil_status"), F.lit("UGIFT"))
    )
    timeline = timeline.withColumn("is_alive", F.col("_s_death_date").isNull())

    # ── SCD Type 2 ────────────────────────────────────────────────────────
    w_scd = Window.partitionBy("person_id").orderBy("event_ts")

    timeline = (
        timeline
        .withColumn("valid_from", F.col("event_ts"))
        .withColumn("valid_to",   F.lead("event_ts").over(w_scd))
        .withColumn("is_current", F.col("valid_to").isNull())
    )

    # ── Velg Silver-schema ────────────────────────────────────────────────
    silver_df = timeline.select(
        F.col("person_id"),
        F.col("_s_fnr_hash").alias("fnr_masked"),
        F.concat_ws(" ", F.col("_s_first_name"), F.col("_s_last_name")).alias("full_name"),
        F.col("_s_first_name").alias("first_name"),
        F.col("_s_last_name").alias("last_name"),
        F.to_date("_s_birth_date").alias("birth_date"),
        F.col("_s_gender").alias("gender"),
        F.col("_s_mother_id").alias("mother_id"),
        F.col("_s_father_id").alias("father_id"),
        F.col("_s_street_address").alias("street_address"),
        F.col("_s_municipality_code").alias("municipality_code"),
        F.col("_s_municipality_name").alias("municipality_name"),
        F.col("_s_county").alias("county"),
        F.col("_s_civil_status").alias("marital_status"),
        F.col("is_alive"),
        F.to_date("_s_death_date").alias("death_date"),
        F.col("event_type").alias("change_event_type"),
        F.col("valid_from"),
        F.col("valid_to"),
        F.col("is_current"),
        F.current_timestamp().alias("_updated_at"),
    )

    count = silver_df.count()
    log.info(f"Skriver {count:,} SCD2-rader til {TARGET_PATH} via MERGE INTO …")

    from delta.tables import DeltaTable

    # Bruk overwrite ved første kjøring eller ved skjemaendring (f.eks. omdøpte kolonner),
    # ellers MERGE INTO for atomisk inkrementell oppdatering.
    needs_overwrite = True
    if DeltaTable.isDeltaTable(spark, TARGET_PATH):
        existing_cols = set(spark.read.format("delta").load(TARGET_PATH).columns)
        needs_overwrite = not set(silver_df.columns).issubset(existing_cols)

    if needs_overwrite:
        log.info("Skjema endret eller første kjøring — bruker overwrite.")
        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("is_current")
            .save(TARGET_PATH)
        )
    else:
        log.info("Skjema uendret — bruker MERGE INTO.")
        dt = DeltaTable.forPath(spark, TARGET_PATH)
        (
            dt.alias("t")
            .merge(
                silver_df.alias("s"),
                "t.person_id = s.person_id AND t.valid_from = s.valid_from",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )
    log.info("Done.")


# ── Inngangspunkt ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--minio-endpoint",   default=os.getenv("MINIO_ENDPOINT",    "http://minio.slettix-analytics.svc.cluster.local:9000"))
    parser.add_argument("--minio-access-key", default=os.getenv("AWS_ACCESS_KEY_ID", "admin"))
    parser.add_argument("--minio-secret-key", default=os.getenv("AWS_SECRET_ACCESS_KEY", "changeme"))
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("folkeregister_person_registry")
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

    build_registry(spark)
    spark.stop()


if __name__ == "__main__":
    main()
