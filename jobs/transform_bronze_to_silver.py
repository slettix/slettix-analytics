"""
Transform Bronze Delta table → Silver Delta table.

Applies null handling and type casting rules from a config file,
then upserts the result into Silver using Delta merge on the primary key.

Usage:
  spark-submit /opt/spark/jobs/transform_bronze_to_silver.py \\
    --config /opt/spark/conf/silver/employees.json

Config supports two source-formats:

  1. Flat Bronze (default):
     {
       "source":      "s3a://bronze/employees",
       "target":      "s3a://silver/employees",
       "primary_key": "id",
       "null_handling": {
         "drop_if_null": ["id", "name"],
         "fill":         {"department": "unknown", "salary": 0}
       },
       "cast": {
         "id": "integer",
         "salary": "integer",
         "hire_date": "date"
       }
     }

  2. IDP-format (payload_json-flatten — SILVER-4):
     {
       "source":      "s3a://bronze/folkeregister/person_events",
       "target":      "s3a://silver/folkeregister/persons",
       "primary_key": "citizen_id",
       "payload_extract": {
         "from_column": "payload_json",
         "fields": {
           "citizen_id":   "$.citizenId",
           "first_name":   "$.firstName",
           "municipality": "$.municipalityCode"
         }
       },
       "cast":          { "citizen_id": "string" },
       "null_handling": { "drop_if_null": ["citizen_id"] }
     }

  Når `payload_extract` finnes, ekstraheres JSON-feltene fra angitt
  kolonne FØR cast/null_handling. Resterende kolonner (event_type osv)
  kan også med — bare uten å ekstrahere fra dem.
"""

import argparse
import json
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_logger import get_logger

log = get_logger("transform_bronze_to_silver")

# Bronze metadata columns that should not carry over to Silver
BRONZE_META_COLS = {"ingestion_date", "_source_path", "_ingested_at"}


def apply_casts(df: DataFrame, cast_rules: dict) -> DataFrame:
    """
    Cast columns using try_cast so bad values become null (rather than crashing).
    Logs the number of nulls introduced per column.
    """
    for col_name, target_type in cast_rules.items():
        if col_name not in df.columns:
            log.warning(f"Cast rule references missing column '{col_name}' — skipping")
            continue

        before_nulls = df.filter(F.col(col_name).isNull()).count()
        df = df.withColumn(col_name, F.col(col_name).cast(target_type))
        after_nulls = df.filter(F.col(col_name).isNull()).count()

        introduced = after_nulls - before_nulls
        if introduced > 0:
            log.warning(f"Cast '{col_name}' → {target_type}: {introduced} value(s) could not be cast and became null")
        else:
            log.info(f"Cast '{col_name}' → {target_type}: OK")

    return df


def apply_payload_extract(df: DataFrame, payload_extract: dict) -> DataFrame:
    """SILVER-4: Trekk ut top-level JSON-felter fra en payload-kolonne.

    Erstatter rad-settet med kun de ekstraherte feltene (pluss Bronze
    metadata `_ingested_at`/`event_type` om de finnes — de brukes senere
    av dedup-vinduet og slettes så av drop_bronze_metadata).
    """
    from_col = payload_extract.get("from_column", "payload_json")
    fields   = payload_extract.get("fields", {})
    if from_col not in df.columns:
        log.warning(f"payload_extract.from_column '{from_col}' finnes ikke — hopper over flatten")
        return df

    select_exprs = []
    for target_name, json_path in fields.items():
        select_exprs.append(F.get_json_object(F.col(from_col), json_path).alias(target_name))

    # Behold _ingested_at (brukes til dedup) og event_type hvis tilgjengelige.
    for passthrough in ("_ingested_at", "event_type", "event_timestamp"):
        if passthrough in df.columns:
            select_exprs.append(F.col(passthrough))

    extracted = df.select(*select_exprs)
    log.info(f"Ekstraherte {len(fields)} payload-felter fra '{from_col}': {list(fields.keys())}")
    return extracted


def apply_null_rules(df: DataFrame, null_handling: dict) -> DataFrame:
    """Apply drop and fill rules for null values."""
    drop_cols = null_handling.get("drop_if_null", [])
    fill_map  = null_handling.get("fill", {})

    if drop_cols:
        before = df.count()
        df = df.dropna(subset=drop_cols)
        dropped = before - df.count()
        if dropped:
            log.warning(f"Dropped {dropped} row(s) with nulls in: {drop_cols}")

    if fill_map:
        df = df.fillna(fill_map)
        log.info(f"Filled nulls: {fill_map}")

    return df


def drop_bronze_metadata(df: DataFrame) -> DataFrame:
    """Remove Bronze-specific metadata columns from Silver data."""
    cols_to_drop = [c for c in BRONZE_META_COLS if c in df.columns]
    return df.drop(*cols_to_drop)


def add_silver_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("_silver_updated_at", F.current_timestamp())


def upsert_to_silver(spark: SparkSession, df: DataFrame, target: str, primary_key: str) -> None:  # pragma: no cover
    """
    Merge df into the Silver Delta table on primary_key.
    - Matching rows are updated.
    - New rows are inserted.
    """
    from delta.tables import DeltaTable  # lazy — only needed at runtime with Delta JARs

    if DeltaTable.isDeltaTable(spark, target):
        silver = DeltaTable.forPath(spark, target)
        (
            silver.alias("existing")
            .merge(
                df.alias("incoming"),
                f"existing.{primary_key} = incoming.{primary_key}",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        log.info(f"Merged {df.count()} rows into {target}")
    else:
        df.write.format("delta").save(target)
        log.info(f"Created Silver table at {target} with {df.count()} rows")


def _normalize_s3_path(path: str) -> str:
    """Normaliser s3:// → s3a:// — jobben har kun S3A-driveren konfigurert
    via spark.hadoop.fs.s3a.*. Wizarden lagrer paths som s3:// i configen,
    så vi normaliserer ved bruk i stedet for å kreve at den endres."""
    if isinstance(path, str) and path.startswith("s3://"):
        return "s3a://" + path[len("s3://"):]
    return path


def run(spark: SparkSession, config: dict) -> None:
    source           = _normalize_s3_path(config["source"])
    target           = _normalize_s3_path(config["target"])
    primary_key      = config["primary_key"]
    null_rules       = config.get("null_handling", {})
    cast_rules       = config.get("cast", {})
    payload_extract  = config.get("payload_extract")
    t0               = time.time()

    log.info("Job started", extra={"event": "job_start", "source": source, "target": target})

    df = spark.read.format("delta").load(source)
    rows_read = df.count()
    log.info("Bronze read", extra={"event": "read_done", "rows_read": rows_read, "source": source})

    if payload_extract:
        df = apply_payload_extract(df, payload_extract)

    df = apply_casts(df, cast_rules)
    df = apply_null_rules(df, null_rules)

    # Keep only the latest record per primary key to satisfy Delta merge's
    # requirement that each target row matches at most one source row.
    # Must happen before drop_bronze_metadata so _ingested_at is still available.
    from pyspark.sql.window import Window
    if "_ingested_at" in df.columns:
        order_col = F.col("_ingested_at").desc()
    elif "event_timestamp" in df.columns:
        order_col = F.col("event_timestamp").desc()
    else:
        order_col = F.lit(0)  # fallback — beholder vilkårlig rad per pk
    df = (
        df.withColumn("_rn", F.row_number().over(
            Window.partitionBy(primary_key).orderBy(order_col)
        ))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    df = drop_bronze_metadata(df)
    df = add_silver_metadata(df)

    rows_out = df.count()
    upsert_to_silver(spark, df, target, primary_key)
    log.info("Job completed", extra={
        "event": "job_end", "rows_read": rows_read, "rows_written": rows_out,
        "target": target, "elapsed_s": round(time.time() - t0, 2),
    })


def _load_config(spark, path: str) -> dict:
    """Les config-JSON fra lokal sti eller S3/S3A (SILVER-3).

    For S3-stier brukes Spark/Hadoop S3A — som allerede er konfigurert
    via spark.hadoop.fs.s3a.*-conf — slik at vi unngår en separat
    boto3-avhengighet i Spark-imaget."""
    if path.startswith(("s3://", "s3a://")):
        # S3A er den eneste konfigurerte S3-driveren i jobben; normaliser
        # s3:// → s3a:// så Hadoop ikke faller tilbake på s3n eller feiler.
        if path.startswith("s3://"):
            path = "s3a://" + path[len("s3://"):]
        # wholetext må settes via reader-kwarg (ikke .option(...)) — som boolsk
        # via .option() konverteres True → "True", og Spark krever "true".
        content = spark.read.text(path, wholetext=True).collect()[0][0]
        data = json.loads(content)
    else:
        data = json.loads(Path(path).read_text())
    # Silver-wizarden lagrer alltid en wrapper i s3://config/silver/{slug}/current.json:
    #   {"slug": ..., "config": {...selve config...}, "saved_at": ...}
    # (jf. dataportal/main.py:_save_silver_config). Unwrappe når wrapperen er der,
    # så lokale flate test-configer fortsatt funker uten endring.
    if isinstance(data, dict) and "config" in data and isinstance(data["config"], dict):
        return data["config"]
    return data


def main():
    parser = argparse.ArgumentParser(description="Bronze → Silver transformation")
    parser.add_argument("--config", required=True, help="Path to silver config JSON (lokal eller s3a://)")
    args = parser.parse_args()

    # SparkSession må bygges før config kan leses fra S3A — Hadoop-driveren
    # lever i JVM-en og initialiseres med sessionen. AppName avledes derfor
    # fra config-slug-en (foreldermappen) i stedet for target-tabellen.
    config_slug = Path(args.config).parent.name or "config"
    spark = (
        SparkSession.builder
        .appName(f"bronze_to_silver:{config_slug}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    config = _load_config(spark, args.config)
    run(spark, config)
    spark.stop()


if __name__ == "__main__":
    main()
