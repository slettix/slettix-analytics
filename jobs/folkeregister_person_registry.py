"""
Bronze → Silver: Folkeregister personregister (SCD Type 2)

Leser alle personhendelser fra Bronze og bygger et autoritativt personregister
med fullstendig historikk via SCD Type 2.  Tabellen bevarer alle tilstandsendringer
for navn, adresse, vitalstatus og sivilstatus.

SCD2-triggere per person_id (sortert på event_ts):
  citizen.created / event.birth  → oppretter initiell rad
  event.relocation               → ny rad med oppdatert adresse
  citizen.died                   → ny rad med is_alive=false, death_date satt
  event.marriage / event.divorce → ny rad med oppdatert sivilstatus (via family_events)

PII-tiltak:
  Tabellen er «restricted».  Kun fnr_hash lagres — original fnr/ssn lagres aldri.

Bronze-skjema (ny, Pipeline Builder):
  Felter leses fra payload_json (generisk kafka_to_bronze.py-format).

Kjøring:
  spark-submit /opt/spark/jobs/folkeregister_person_registry.py
"""

import argparse
import hashlib
import os
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

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

_sha256_udf = F.udf(
    lambda v: hashlib.sha256(v.encode()).hexdigest() if v else None,
    StringType(),
)


def _nulls(*cols: str):
    return [F.lit(None).cast("string").alias(c) for c in cols]


def _extract_person_fields(df: DataFrame) -> DataFrame:
    """
    Trekker ut domenespesifikke felt fra payload_json.

    Støttede event-typer og payload-felter:
      citizen.created / citizen.died:
        citizenId, ssn, firstName, lastName, sex, birthDate, country, status
      event.birth:
        childId, childSsn, birthDate, parent1Id, parent2Id
      event.relocation:
        citizenId, ssn, streetAddress, municipalityCode, municipalityName, county
    """
    p = "payload_json"

    return df.select(
        F.col("event_type"),
        F.col("event_timestamp"),
        F.col("kafka_timestamp"),

        # person_id: citizenId for citizen-events, childId for birth
        F.coalesce(
            F.get_json_object(p, "$.citizenId"),
            F.get_json_object(p, "$.childId"),
        ).alias("person_id"),

        # fnr_hash: SHA-256 av ssn/childSsn
        _sha256_udf(F.coalesce(
            F.get_json_object(p, "$.ssn"),
            F.get_json_object(p, "$.childSsn"),
        )).alias("fnr_hash"),

        F.get_json_object(p, "$.firstName").alias("first_name"),
        F.get_json_object(p, "$.lastName").alias("last_name"),
        F.get_json_object(p, "$.sex").alias("gender"),
        F.get_json_object(p, "$.birthDate").alias("birth_date"),
        F.get_json_object(p, "$.parent1Id").alias("mother_id"),
        F.get_json_object(p, "$.parent2Id").alias("father_id"),

        # Adressefelter (event.relocation)
        F.get_json_object(p, "$.streetAddress").alias("street_address"),
        F.get_json_object(p, "$.municipalityCode").alias("municipality_code"),
        F.get_json_object(p, "$.municipalityName").alias("municipality_name"),
        F.get_json_object(p, "$.county").alias("county"),

        # death_date: sett for citizen.died
        F.when(
            F.col("event_type") == "citizen.died",
            F.get_json_object(p, "$.birthDate"),
        ).alias("death_date"),

        F.lit(None).cast("string").alias("civil_status_change"),
    )


def _extract_family_fields(df: DataFrame) -> DataFrame:
    """
    Trekker ut domenespesifikke felt fra payload_json for familiehendelser.

    event.marriage / event.divorce:
      citizen1Id, citizen1Ssn, citizen2Id, citizen2Ssn, officialDate
    event.birth:
      childId, childSsn, birthDate, parent1Id, parent2Id
    """
    p = "payload_json"

    return df.select(
        F.col("event_type"),
        F.col("event_timestamp"),
        F.col("kafka_timestamp"),
        F.get_json_object(p, "$.citizen1Id").alias("person_id_a"),
        F.get_json_object(p, "$.citizen2Id").alias("person_id_b"),
        F.get_json_object(p, "$.childId").alias("child_id"),
        F.get_json_object(p, "$.parent1Id").alias("parent1_id"),
        F.get_json_object(p, "$.parent2Id").alias("parent2_id"),
    )


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


def build_registry(spark: SparkSession) -> None:

    log.info("Leser Bronze person_events …")
    raw_person = spark.read.format("delta").load(PERSON_EVENTS_PATH)
    person_df = (
        _extract_person_fields(raw_person)
        .withColumn("event_ts", F.coalesce(
            F.to_timestamp("event_timestamp"), F.col("kafka_timestamp"),
        ))
    )

    try:
        log.info("Leser Bronze family_events …")
        raw_family = spark.read.format("delta").load(FAMILY_EVENTS_PATH)
        family_df = (
            _extract_family_fields(raw_family)
            .withColumn("event_ts", F.coalesce(
                F.to_timestamp("event_timestamp"), F.col("kafka_timestamp"),
            ))
        )
        civil_df = _civil_status_events(family_df)
    except Exception as exc:
        log.warning(f"Fant ikke family_events ({exc}) — sivilstatus settes til UGIFT.")
        civil_df = None

    timeline = _person_state_events(person_df)
    if civil_df is not None:
        timeline = timeline.unionByName(civil_df)

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

    w_scd = Window.partitionBy("person_id").orderBy("event_ts")

    timeline = (
        timeline
        .withColumn("valid_from", F.col("event_ts"))
        .withColumn("valid_to",   F.lead("event_ts").over(w_scd))
        .withColumn("is_current", F.col("valid_to").isNull())
    )

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
