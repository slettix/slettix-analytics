"""
Bronze → Silver: Folkeregister bostedshistorikk

Leser personhendelser fra Bronze og bygger en kronologisk bostedshistorikk
per person.

Hendelseskilder:
  PersonRegistrert — første kjente adresse (innflytting ved registrering)
  PersonFødt       — fødested som første adresse (hvis municipality_* er satt)
  PersonFlyttet    — ny adresse (utløser ny historikkrad)
  PersonDød        — lukker siste bostedsrad (moved_out_date = death_date)

Kolonner:
  person_id           — intern UUID
  municipality_code   — kommunenummer (SSB 4 siffer)
  municipality_name   — kommunenavn
  county              — fylke
  moved_in_date       — dato personen flyttet til kommunen
  moved_out_date      — dato personen flyttet ut (NULL = nåværende adresse)
  is_current          — True hvis gjeldende bosted
  duration_days       — antall dager i kommunen (NULL for is_current=True)
  change_event_type   — kildehendelse (PersonRegistrert / PersonFødt / PersonFlyttet)

Kjøring:
  spark-submit /opt/spark/jobs/folkeregister_residence_history.py
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


log = get_logger("folkeregister_residence_history")

PERSON_EVENTS_PATH  = "s3a://bronze/folkeregister/person_events"
TARGET_PATH         = "s3a://silver/folkeregister/residence_history"
MIGRATION_AGG_PATH  = "s3a://silver/folkeregister/municipality_migration"

# Kun event.relocation inneholder adressedata (newPostalCode / newCity).
# citizen.created og event.birth har ingen adresse i payload.
ADDRESS_EVENTS = {"event.relocation"}


def _extract_fields(df):
    """Trekker ut domenefelt fra payload_json (nytt IDP-format)."""
    p = "payload_json"
    return df.select(
        F.col("event_type"),
        F.col("event_timestamp"),
        F.col("kafka_timestamp"),
        F.coalesce(
            F.get_json_object(p, "$.citizenId"),
            F.get_json_object(p, "$.childId"),
        ).alias("person_id"),
        F.get_json_object(p, "$.municipalityCode").alias("municipality_code"),
        F.get_json_object(p, "$.municipalityName").alias("municipality_name"),
        F.get_json_object(p, "$.county").alias("county"),
        F.when(
            F.col("event_type") == "citizen.died",
            F.get_json_object(p, "$.birthDate"),
        ).alias("death_date"),
    )


def build_residence_history(spark: SparkSession) -> None:

    log.info("Leser Bronze person_events …")
    person_df = (
        spark.read.format("delta").load(PERSON_EVENTS_PATH)
        .transform(_extract_fields)
        .withColumn("event_ts", F.coalesce(
            F.to_timestamp("event_timestamp"), F.col("kafka_timestamp"),
        ))
    )

    # Filtrer til hendelser med adresseinformasjon
    addr_df = person_df.filter(
        F.col("event_type").isin(*ADDRESS_EVENTS) &
        F.col("municipality_code").isNotNull()
    ).select(
        "person_id", "event_ts", "event_type",
        "municipality_code", "municipality_name", "county",
    )

    # ── Finn utflyttingsdato = neste innflyttingsdato for personen ────────
    w = Window.partitionBy("person_id").orderBy("event_ts")

    addr_df = (
        addr_df
        .withColumn("moved_in_ts",  F.col("event_ts"))
        .withColumn("moved_out_ts", F.lead("event_ts").over(w))
        .withColumn("is_current",   F.col("moved_out_ts").isNull())
    )

    # ── Slå opp dødsdato og lukk siste rad hvis personen er død ──────────
    death_df = (
        person_df
        .filter(F.col("event_type") == "citizen.died")
        .select(
            "person_id",
            F.to_date(F.coalesce(F.to_timestamp("death_date"), F.col("event_ts"))).alias("death_date"),
        )
        .dropDuplicates(["person_id"])
    )

    addr_df = addr_df.join(death_df, on="person_id", how="left")

    # Lukk siste rad med dødsdato hvis gjeldende og personen er død
    addr_df = addr_df.withColumn(
        "moved_out_ts",
        F.when(
            F.col("is_current") & F.col("death_date").isNotNull(),
            F.to_timestamp("death_date"),
        ).otherwise(F.col("moved_out_ts")),
    ).withColumn(
        "is_current",
        F.col("is_current") & F.col("death_date").isNull(),
    )

    # ── Beregn duration_days ──────────────────────────────────────────────
    addr_df = addr_df.withColumn(
        "duration_days",
        F.when(
            F.col("moved_out_ts").isNotNull(),
            F.datediff(F.to_date("moved_out_ts"), F.to_date("moved_in_ts")),
        ),
    )

    # ── Velg Silver-schema ────────────────────────────────────────────────
    silver_df = addr_df.select(
        F.col("person_id"),
        F.col("municipality_code"),
        F.col("municipality_name"),
        F.col("county"),
        F.to_date("moved_in_ts").alias("moved_in_date"),
        F.to_date("moved_out_ts").alias("moved_out_date"),
        F.col("is_current"),
        F.col("duration_days"),
        F.col("event_type").alias("change_event_type"),
        F.current_timestamp().alias("_updated_at"),
    )

    count = silver_df.count()
    log.info(f"Skriver {count:,} bostedsrader til {TARGET_PATH} …")

    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("is_current")
        .save(TARGET_PATH)
    )

    # ── Aggregat på kommunenivå: antall inn- og utflyttinger per periode ──
    log.info("Beregner municipality-migrasjon aggregat …")

    period_col = F.date_trunc("month", "moved_in_date").alias("period")

    innflyttinger = (
        silver_df
        .withColumn("period", F.date_trunc("month", "moved_in_date"))
        .groupBy("municipality_code", "municipality_name", "county", "period")
        .agg(F.count("*").alias("innflyttinger"))
    )

    utflyttinger = (
        silver_df
        .filter(F.col("moved_out_date").isNotNull())
        .withColumn("period", F.date_trunc("month", "moved_out_date"))
        .groupBy("municipality_code", "municipality_name", "county", "period")
        .agg(F.count("*").alias("utflyttinger"))
    )

    migration_agg = (
        innflyttinger.alias("inn")
        .join(
            utflyttinger.alias("ut"),
            on=["municipality_code", "municipality_name", "county", "period"],
            how="full",
        )
        .select(
            F.coalesce(F.col("inn.municipality_code"), F.col("ut.municipality_code")).alias("municipality_code"),
            F.coalesce(F.col("inn.municipality_name"), F.col("ut.municipality_name")).alias("municipality_name"),
            F.coalesce(F.col("inn.county"),            F.col("ut.county")).alias("county"),
            F.coalesce(F.col("inn.period"),            F.col("ut.period")).alias("period"),
            F.coalesce(F.col("inn.innflyttinger"),     F.lit(0)).alias("innflyttinger"),
            F.coalesce(F.col("ut.utflyttinger"),       F.lit(0)).alias("utflyttinger"),
        )
        .withColumn("netto", F.col("innflyttinger") - F.col("utflyttinger"))
        .withColumn("_updated_at", F.current_timestamp())
    )

    agg_count = migration_agg.count()
    log.info(f"Skriver {agg_count:,} aggregatrader til {MIGRATION_AGG_PATH} …")

    (
        migration_agg.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("municipality_code")
        .save(MIGRATION_AGG_PATH)
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
        .appName("folkeregister_residence_history")
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

    build_residence_history(spark)
    spark.stop()


if __name__ == "__main__":
    main()
