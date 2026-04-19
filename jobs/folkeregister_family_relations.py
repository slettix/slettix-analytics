"""
Bronze → Silver: Folkeregister familierelasjoner

Leser familiehendelser fra Bronze og bygger en relasjons-tabell (kant-liste)
over ekteskap, skilsmisser og foreldre–barn-relasjoner.

Relasjontyper:
  GIFT          — ektepar (person_id_a ↔ person_id_b), SCD2 med valid_from/valid_to
  SKILT         — tidligere ektepar (lukkes ved EkteskapOppløst)
  FORELDER_AV   — foreldre–barn og forfader–etterkommer (alle generasjoner)
  BARN_AV       — barn–forelder og etterkommer–forfader (alle generasjoner)

Generasjonsdybde:
  generation_depth = 1 for direkte foreldre–barn
  generation_depth = 2 for besteforeldre–barnebarn, osv.
  Beregnes iterativt (transitivt lukking) opp til MAX_ANCESTOR_DEPTH generasjoner.

Kjøring:
  spark-submit /opt/spark/jobs/folkeregister_family_relations.py
"""

import argparse
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

try:
    from spark_logger import get_logger
except ImportError:
    import logging

    def get_logger(name):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)


log = get_logger("folkeregister_family_relations")

FAMILY_EVENTS_PATH  = "s3a://bronze/folkeregister/family_events"
TARGET_PATH         = "s3a://silver/folkeregister/family_relations"
MAX_ANCESTOR_DEPTH  = 5


def _extract_family_fields(df: DataFrame) -> DataFrame:
    """
    Trekker ut domenespesifikke felt fra payload_json.

    event.marriage / event.divorce:
      citizen1Id → person_id_a, citizen2Id → person_id_b
    event.birth:
      childId → child_id, parent1Id → mother_id, parent2Id → father_id
    """
    p = "payload_json"
    return df.select(
        F.col("event_type"),
        F.col("event_timestamp"),
        F.col("kafka_timestamp"),
        F.get_json_object(p, "$.citizen1Id").alias("person_id_a"),
        F.get_json_object(p, "$.citizen2Id").alias("person_id_b"),
        F.get_json_object(p, "$.childId").alias("child_id"),
        F.get_json_object(p, "$.parent1Id").alias("mother_id"),
        F.get_json_object(p, "$.parent2Id").alias("father_id"),
    )


def build_relations(spark: SparkSession) -> None:

    log.info("Leser Bronze family_events …")
    raw_family = spark.read.format("delta").load(FAMILY_EVENTS_PATH)
    family_df = (
        _extract_family_fields(raw_family)
        .withColumn("event_ts", F.coalesce(
            F.to_timestamp("event_timestamp"), F.col("kafka_timestamp"),
        ))
    )

    frames = []

    # ── Ekteskap (SCD2: åpnes ved event.marriage, lukkes ved event.divorce) ──
    married = family_df.filter(F.col("event_type") == "event.marriage")

    # Finn tilhørende skilsmisse for hvert ektepar
    divorce_lookup = (
        family_df.filter(F.col("event_type") == "event.divorce")
        .select(
            F.col("person_id_a").alias("d_a"),
            F.col("person_id_b").alias("d_b"),
            F.col("event_ts").alias("divorce_ts"),
        )
    )

    # Gjør join på begge partskombinasjoner (a↔b og b↔a)
    marriage_edges = (
        married.alias("m")
        .join(
            divorce_lookup.alias("d"),
            (
                (F.col("m.person_id_a") == F.col("d.d_a")) &
                (F.col("m.person_id_b") == F.col("d.d_b"))
            ) | (
                (F.col("m.person_id_a") == F.col("d.d_b")) &
                (F.col("m.person_id_b") == F.col("d.d_a"))
            ),
            how="left",
        )
        .select(
            F.concat_ws("_", F.col("m.person_id_a"), F.col("m.person_id_b"), F.col("m.event_ts").cast("string"))
             .alias("relation_id"),
            F.col("m.person_id_a").alias("person_id_a"),
            F.col("m.person_id_b").alias("person_id_b"),
            F.lit("GIFT").alias("relation_type"),
            F.lit(None).cast("integer").alias("generation_depth"),
            F.col("m.event_ts").alias("valid_from"),
            F.col("d.divorce_ts").alias("valid_to"),
            F.col("d.divorce_ts").isNull().alias("is_current"),
        )
    )
    frames.append(marriage_edges)

    # ── Skilsmisse-kanter (lukket relasjon) ───────────────────────────────
    divorce_edges = (
        family_df.filter(F.col("event_type") == "event.divorce")
        .select(
            F.concat_ws("_", F.col("person_id_a"), F.col("person_id_b"), F.col("event_ts").cast("string"))
             .alias("relation_id"),
            F.col("person_id_a"),
            F.col("person_id_b"),
            F.lit("SKILT").alias("relation_type"),
            F.lit(None).cast("integer").alias("generation_depth"),
            F.col("event_ts").alias("valid_from"),
            F.lit(None).cast("timestamp").alias("valid_to"),
            F.lit(True).alias("is_current"),
        )
    )
    frames.append(divorce_edges)

    # ── Foreldre–barn-kanter (fra event.birth) ───────────────────────────
    births = family_df.filter(F.col("event_type") == "event.birth")

    # Direkte foreldre–barn (depth=1)
    direct_pc_frames = []
    for parent_col in ["mother_id", "father_id"]:
        direct_pc_frames.append(
            births.filter(F.col(parent_col).isNotNull())
            .select(
                F.col(parent_col).alias("ancestor_id"),
                F.col("child_id").alias("descendant_id"),
                F.col("event_ts"),
            )
        )

    from functools import reduce as _reduce
    direct_pc = _reduce(DataFrame.unionByName, direct_pc_frames).dropDuplicates(["ancestor_id", "descendant_id"])

    # ── Transitivt lukking: beregn generasjonsdybde opp til MAX_ANCESTOR_DEPTH ──
    all_ancestors = direct_pc.withColumn("generation_depth", F.lit(1))

    for depth in range(2, MAX_ANCESTOR_DEPTH + 1):
        prev_level = all_ancestors.filter(F.col("generation_depth") == depth - 1)
        next_level = (
            prev_level.alias("prev")
            .join(direct_pc.alias("dc"), F.col("prev.ancestor_id") == F.col("dc.descendant_id"))
            .select(
                F.col("dc.ancestor_id"),
                F.col("prev.descendant_id"),
                F.col("prev.event_ts"),
                F.lit(depth).alias("generation_depth"),
            )
        )
        # Stopp hvis ingen nye kanter
        if next_level.count() == 0:
            break
        # Unngå sykluser
        existing_pairs = all_ancestors.select("ancestor_id", "descendant_id")
        next_level = next_level.join(
            existing_pairs,
            on=["ancestor_id", "descendant_id"],
            how="left_anti",
        )
        if next_level.count() == 0:
            break
        all_ancestors = all_ancestors.union(next_level)

    # Bygg FORELDER_AV-kanter (forfader → etterkommer) for alle dybder
    for _, row_depth in all_ancestors.select("generation_depth").distinct().collect():
        depth_df = all_ancestors.filter(F.col("generation_depth") == row_depth)
        frames.append(
            depth_df.select(
                F.concat_ws("_", F.col("ancestor_id"), F.col("descendant_id"), F.lit("FORELDER_AV"), F.lit(row_depth).cast("string"))
                 .alias("relation_id"),
                F.col("ancestor_id").alias("person_id_a"),
                F.col("descendant_id").alias("person_id_b"),
                F.lit("FORELDER_AV").alias("relation_type"),
                F.lit(row_depth).alias("generation_depth"),
                F.col("event_ts").alias("valid_from"),
                F.lit(None).cast("timestamp").alias("valid_to"),
                F.lit(True).alias("is_current"),
            )
        )
        # Speilkant: BARN_AV
        frames.append(
            depth_df.select(
                F.concat_ws("_", F.col("descendant_id"), F.col("ancestor_id"), F.lit("BARN_AV"), F.lit(row_depth).cast("string"))
                 .alias("relation_id"),
                F.col("descendant_id").alias("person_id_a"),
                F.col("ancestor_id").alias("person_id_b"),
                F.lit("BARN_AV").alias("relation_type"),
                F.lit(row_depth).alias("generation_depth"),
                F.col("event_ts").alias("valid_from"),
                F.lit(None).cast("timestamp").alias("valid_to"),
                F.lit(True).alias("is_current"),
            )
        )

    # ── Union + legg til metadata ─────────────────────────────────────────
    relations_df = _reduce(DataFrame.unionByName, frames)
    relations_df = relations_df.withColumn("_updated_at", F.current_timestamp())

    count = relations_df.count()
    log.info(f"Skriver {count:,} relasjonrader til {TARGET_PATH} …")

    (
        relations_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("relation_type")
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
        .appName("folkeregister_family_relations")
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

    build_relations(spark)
    spark.stop()


if __name__ == "__main__":
    main()
