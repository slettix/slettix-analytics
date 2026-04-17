"""
Folkeregister Bronze Seeder — syntetiske testdata

Genererer realistiske syntetiske Bronze-tabeller for person_events og family_events
slik at hele folkeregister-pipelinen (Silver + Gold) kan testes i et dev-miljø uten
tilgang til en live Kafka-strøm.

Produserer:
  s3a://bronze/folkeregister/person_events   (~2 000 rader)
  s3a://bronze/folkeregister/family_events   (~600 rader)

Kjøring:
  spark-submit /opt/spark/jobs/folkeregister_seed_bronze.py
  spark-submit /opt/spark/jobs/folkeregister_seed_bronze.py --persons 5000
"""

import argparse
import hashlib
import os
import random
import uuid
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

try:
    from spark_logger import get_logger
except ImportError:
    import logging

    def get_logger(name):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)


log = get_logger("folkeregister_seed_bronze")

PERSON_EVENTS_PATH = "s3a://bronze/folkeregister/person_events"
FAMILY_EVENTS_PATH = "s3a://bronze/folkeregister/family_events"

# ── Norske navnedata ────────────────────────────────────────────────────────

FIRST_NAMES_M = [
    "Ole", "Lars", "Bjørn", "Eirik", "Tor", "Håkon", "Magnus", "Kristian",
    "Anders", "Nils", "Per", "Jonas", "Henrik", "Morten", "Trond", "Espen",
    "Rune", "Terje", "Geir", "Steinar", "Svein", "Knut", "Helge", "Arild",
]
FIRST_NAMES_F = [
    "Kari", "Anne", "Mari", "Ingrid", "Astrid", "Sigrid", "Hilde", "Ragnhild",
    "Marit", "Toril", "Tone", "Bente", "Gro", "Sissel", "Berit", "Nina",
    "Helene", "Christine", "Anita", "Camilla", "Lise", "Mette", "Siri", "Tove",
]
LAST_NAMES = [
    "Hansen", "Johansen", "Olsen", "Larsen", "Andersen", "Pedersen", "Nilsen",
    "Kristiansen", "Jensen", "Karlsen", "Johnsen", "Pettersen", "Eriksen",
    "Berg", "Haugen", "Holm", "Lie", "Moen", "Strand", "Bakke",
    "Dahl", "Lund", "Halvorsen", "Sørensen", "Jakobsen", "Thorsen", "Ottesen",
]

MUNICIPALITIES = [
    ("0301", "Oslo",        "Oslo"),
    ("4601", "Bergen",      "Vestland"),
    ("5001", "Trondheim",   "Trøndelag"),
    ("1103", "Stavanger",   "Rogaland"),
    ("3005", "Drammen",     "Viken"),
    ("3201", "Lillehammer", "Innlandet"),
    ("1804", "Bodø",        "Nordland"),
    ("1902", "Tromsø",      "Troms og Finnmark"),
    ("1001", "Kristiansand","Agder"),
    ("3801", "Skien",       "Telemark"),
]

RNG = random.Random(42)


# ── Hjelpefunksjoner ────────────────────────────────────────────────────────

def _rand_date(start: date, end: date) -> date:
    return start + timedelta(days=RNG.randint(0, (end - start).days))


def _ts(d: date) -> str:
    """ISO-8601 timestamp string (Spark leser dette som TimestampType)."""
    return datetime(d.year, d.month, d.day, RNG.randint(6, 22), RNG.randint(0, 59), tzinfo=timezone.utc).isoformat()


def _fnr_hash(person_id: str) -> str:
    """SHA-256 av en deterministisk SSN-lookalike (kun for seeding — ikke ekte fnr)."""
    fake_ssn = f"SEED-{person_id}"
    return hashlib.sha256(fake_ssn.encode()).hexdigest()


def _municipality() -> tuple:
    return RNG.choice(MUNICIPALITIES)


# ── Bygg person_events ──────────────────────────────────────────────────────

def build_person_events(n_persons: int) -> list[dict]:
    rows = []
    ref_start = date(2015, 1, 1)
    ref_end   = date(2024, 12, 31)

    for _ in range(n_persons):
        person_id = str(uuid.uuid4())
        gender    = RNG.choice(["M", "F"])
        first_name = RNG.choice(FIRST_NAMES_M if gender == "M" else FIRST_NAMES_F)
        last_name  = RNG.choice(LAST_NAMES)
        birth_date = _rand_date(date(1940, 1, 1), date(2005, 12, 31))
        muni       = _municipality()

        # Registreringshendelse
        reg_date = _rand_date(ref_start, min(ref_end, date(2023, 1, 1)))
        rows.append({
            "kafka_topic":     "folkeregister.citizen.created",
            "kafka_partition": RNG.randint(0, 3),
            "kafka_offset":    RNG.randint(0, 1_000_000),
            "kafka_timestamp": _ts(reg_date),
            "event_id":        str(uuid.uuid4()),
            "event_type":      "citizen.created",
            "event_timestamp": _ts(reg_date).isoformat(),
            "person_id":       person_id,
            "fnr_hash":        _fnr_hash(person_id),
            "first_name":      first_name,
            "last_name":       last_name,
            "birth_date":      birth_date.isoformat(),
            "gender":          gender,
            "mother_id":       None,
            "father_id":       None,
            "death_date":      None,
            "municipality_code": muni[0],
            "municipality_name": muni[1],
            "county":            muni[2],
            "_processed_at":   _ts(reg_date),
            "_source_topics":  "folkeregister.citizen.created",
            "event_date":      reg_date.isoformat(),
        })

        # 30% sjanse for relocation-hendelse
        if RNG.random() < 0.30:
            move_date = _rand_date(reg_date + timedelta(days=90), ref_end)
            new_muni  = _municipality()
            rows.append({
                "kafka_topic":     "folkeregister.event.relocation",
                "kafka_partition": RNG.randint(0, 3),
                "kafka_offset":    RNG.randint(0, 1_000_000),
                "kafka_timestamp": _ts(move_date),
                "event_id":        str(uuid.uuid4()),
                "event_type":      "event.relocation",
                "event_timestamp": _ts(move_date).isoformat(),
                "person_id":       person_id,
                "fnr_hash":        _fnr_hash(person_id),
                "first_name":      first_name,
                "last_name":       last_name,
                "birth_date":      birth_date.isoformat(),
                "gender":          gender,
                "mother_id":       None,
                "father_id":       None,
                "death_date":      None,
                "municipality_code": new_muni[0],
                "municipality_name": new_muni[1],
                "county":            new_muni[2],
                "_processed_at":   _ts(move_date),
                "_source_topics":  "folkeregister.event.relocation",
                "event_date":      move_date.isoformat(),
            })

        # 5% sjanse for dødshendelse
        if RNG.random() < 0.05:
            death_date = _rand_date(reg_date + timedelta(days=180), ref_end)
            rows.append({
                "kafka_topic":     "folkeregister.citizen.died",
                "kafka_partition": RNG.randint(0, 3),
                "kafka_offset":    RNG.randint(0, 1_000_000),
                "kafka_timestamp": _ts(death_date),
                "event_id":        str(uuid.uuid4()),
                "event_type":      "citizen.died",
                "event_timestamp": _ts(death_date).isoformat(),
                "person_id":       person_id,
                "fnr_hash":        _fnr_hash(person_id),
                "first_name":      first_name,
                "last_name":       last_name,
                "birth_date":      birth_date.isoformat(),
                "gender":          gender,
                "mother_id":       None,
                "father_id":       None,
                "death_date":      death_date.isoformat(),
                "municipality_code": muni[0],
                "municipality_name": muni[1],
                "county":            muni[2],
                "_processed_at":   _ts(death_date),
                "_source_topics":  "folkeregister.citizen.died",
                "event_date":      death_date.isoformat(),
            })

    return rows


# ── Bygg family_events ──────────────────────────────────────────────────────

def build_family_events(person_ids: list[str], n_marriages: int) -> list[dict]:
    rows = []
    ref_start = date(2015, 1, 1)
    ref_end   = date(2024, 12, 31)

    shuffled = list(person_ids)
    RNG.shuffle(shuffled)

    # Danner par — ingen overlapp
    pairs = [(shuffled[i], shuffled[i + 1]) for i in range(0, min(n_marriages * 2, len(shuffled) - 1), 2)]

    for pid_a, pid_b in pairs:
        marry_date = _rand_date(ref_start, ref_end - timedelta(days=180))
        rows.append({
            "kafka_topic":     "folkeregister.event.marriage",
            "kafka_partition": RNG.randint(0, 3),
            "kafka_offset":    RNG.randint(0, 1_000_000),
            "kafka_timestamp": _ts(marry_date),
            "event_id":        str(uuid.uuid4()),
            "event_type":      "event.marriage",
            "event_timestamp": _ts(marry_date).isoformat(),
            "person_id_a":     pid_a,
            "person_id_b":     pid_b,
            "official_date":   marry_date.isoformat(),
            "child_id":        None,
            "birth_date":      None,
            "_processed_at":   _ts(marry_date),
            "_source_topics":  "folkeregister.event.marriage",
            "event_date":      marry_date.isoformat(),
        })

        # 20% sjanse for skilsmisse (kun hvis det er plass til minst 1 år + buffer)
        divorce_start = marry_date + timedelta(days=365)
        if RNG.random() < 0.20 and divorce_start < ref_end:
            div_date = _rand_date(divorce_start, ref_end)
            rows.append({
                "kafka_topic":     "folkeregister.event.divorce",
                "kafka_partition": RNG.randint(0, 3),
                "kafka_offset":    RNG.randint(0, 1_000_000),
                "kafka_timestamp": _ts(div_date),
                "event_id":        str(uuid.uuid4()),
                "event_type":      "event.divorce",
                "event_timestamp": _ts(div_date).isoformat(),
                "person_id_a":     pid_a,
                "person_id_b":     pid_b,
                "official_date":   div_date.isoformat(),
                "child_id":        None,
                "birth_date":      None,
                "_processed_at":   _ts(div_date),
                "_source_topics":  "folkeregister.event.divorce",
                "event_date":      div_date.isoformat(),
            })

        # 40% sjanse for barn
        if RNG.random() < 0.40:
            child_date = _rand_date(marry_date + timedelta(days=90), ref_end)
            child_id   = str(uuid.uuid4())
            rows.append({
                "kafka_topic":     "folkeregister.event.birth",
                "kafka_partition": RNG.randint(0, 3),
                "kafka_offset":    RNG.randint(0, 1_000_000),
                "kafka_timestamp": _ts(child_date),
                "event_id":        str(uuid.uuid4()),
                "event_type":      "event.birth",
                "event_timestamp": _ts(child_date).isoformat(),
                "person_id_a":     pid_a,
                "person_id_b":     pid_b,
                "official_date":   None,
                "child_id":        child_id,
                "birth_date":      child_date.isoformat(),
                "_processed_at":   _ts(child_date),
                "_source_topics":  "folkeregister.event.birth",
                "event_date":      child_date.isoformat(),
            })

    return rows


# ── Eksplisitte skjema (unngår PySpark type-inferens-feil) ───────────────────

_S = StringType()

PERSON_SCHEMA = StructType([
    StructField("kafka_topic",      _S),
    StructField("kafka_partition",  IntegerType()),
    StructField("kafka_offset",     LongType()),
    StructField("kafka_timestamp",  _S),
    StructField("event_id",         _S),
    StructField("event_type",       _S),
    StructField("event_timestamp",  _S),
    StructField("person_id",        _S),
    StructField("fnr_hash",         _S),
    StructField("first_name",       _S),
    StructField("last_name",        _S),
    StructField("birth_date",       _S),
    StructField("gender",           _S),
    StructField("mother_id",        _S),
    StructField("father_id",        _S),
    StructField("death_date",       _S),
    StructField("municipality_code", _S),
    StructField("municipality_name", _S),
    StructField("county",           _S),
    StructField("_processed_at",    _S),
    StructField("_source_topics",   _S),
    StructField("event_date",       _S),
])

FAMILY_SCHEMA = StructType([
    StructField("kafka_topic",      _S),
    StructField("kafka_partition",  IntegerType()),
    StructField("kafka_offset",     LongType()),
    StructField("kafka_timestamp",  _S),
    StructField("event_id",         _S),
    StructField("event_type",       _S),
    StructField("event_timestamp",  _S),
    StructField("person_id_a",      _S),
    StructField("person_id_b",      _S),
    StructField("official_date",    _S),
    StructField("child_id",         _S),
    StructField("birth_date",       _S),
    StructField("_processed_at",    _S),
    StructField("_source_topics",   _S),
    StructField("event_date",       _S),
])


def _rows_to_tuples(rows: list[dict], schema: StructType) -> list[tuple]:
    """Konverterer dict-liste til tuple-liste i skjema-kolonnerekkefølge."""
    cols = [f.name for f in schema.fields]
    return [tuple(r.get(c) for c in cols) for r in rows]


# ── Hovednlogikk ─────────────────────────────────────────────────────────────

def seed(spark: SparkSession, n_persons: int) -> None:
    log.info(f"Genererer {n_persons} syntetiske personhendelser …")
    person_rows = build_person_events(n_persons)
    person_ids  = list({r["person_id"] for r in person_rows if r["event_type"] == "citizen.created"})

    n_marriages = max(1, len(person_ids) // 4)
    log.info(f"Genererer ~{n_marriages} ekteskaps-/familiehendelser …")
    family_rows = build_family_events(person_ids, n_marriages)

    person_df = (
        spark.createDataFrame(_rows_to_tuples(person_rows, PERSON_SCHEMA), PERSON_SCHEMA)
        .withColumn("kafka_timestamp", F.to_timestamp("kafka_timestamp"))
        .withColumn("_processed_at",   F.to_timestamp("_processed_at"))
        .withColumn("event_date",      F.to_date("event_date"))
    )

    family_df = (
        spark.createDataFrame(_rows_to_tuples(family_rows, FAMILY_SCHEMA), FAMILY_SCHEMA)
        .withColumn("kafka_timestamp", F.to_timestamp("kafka_timestamp"))
        .withColumn("_processed_at",   F.to_timestamp("_processed_at"))
        .withColumn("event_date",      F.to_date("event_date"))
    )

    log.info(f"Skriver {person_df.count()} rader til {PERSON_EVENTS_PATH} …")
    (
        person_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("event_type", "event_date")
        .save(PERSON_EVENTS_PATH)
    )

    log.info(f"Skriver {family_df.count()} rader til {FAMILY_EVENTS_PATH} …")
    (
        family_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("event_type", "event_date")
        .save(FAMILY_EVENTS_PATH)
    )

    log.info("Seeding fullført.")


# ── Inngangspunkt ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Folkeregister Bronze Seeder — syntetiske testdata")
    parser.add_argument("--persons",           type=int, default=2000)
    parser.add_argument("--minio-endpoint",    default=os.getenv("MINIO_ENDPOINT",    "http://minio.slettix-analytics.svc.cluster.local:9000"))
    parser.add_argument("--minio-access-key",  default=os.getenv("AWS_ACCESS_KEY_ID", "admin"))
    parser.add_argument("--minio-secret-key",  default=os.getenv("AWS_SECRET_ACCESS_KEY", "changeme"))
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("folkeregister_seed_bronze")
        .config("spark.sql.extensions",           "io.delta.sql.DeltaSparkSessionExtension")
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

    seed(spark, args.persons)
    spark.stop()


if __name__ == "__main__":
    main()
