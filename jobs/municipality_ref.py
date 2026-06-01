"""
municipality_ref — slå opp kommunenavn + fylke fra Silver-referansetabellen
(s3://silver/reference/kommuner) for berikelse av adresse-events.

Brukes av folkeregister_residence_history.py — tabellen lastes av
load_kommuner_reference-task-en i folkeregister_silver-DAGen.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


REFERENCE_TABLE = "s3a://silver/reference/kommuner"


def enrich_municipality(addr_df: DataFrame, spark: SparkSession) -> DataFrame:
    """Berik `addr_df` med korrigert `municipality_name` og `county` (fylke)
    basert på `municipality_code` slått opp mot kommuner-referansetabellen.

    Faller tilbake til eksisterende verdier hvis koden ikke finnes i tabellen,
    så pipeline ikke tap rader ved ukjent kommunenr.
    """
    ref = (
        spark.read.format("delta").load(REFERENCE_TABLE)
        .select(
            F.col("kommunenr").alias("_ref_code"),
            F.col("kommunenavn").alias("_ref_name"),
            F.col("fylke").alias("_ref_county"),
        )
    )
    # Sørg for konsistent zfill(4)-format på begge sider av joinet.
    enriched = (
        addr_df
        .withColumn("_join_code", F.lpad(F.col("municipality_code").cast("string"), 4, "0"))
        .join(ref, F.col("_join_code") == F.col("_ref_code"), "left")
        .withColumn("municipality_name", F.coalesce(F.col("_ref_name"), F.col("municipality_name")))
        .withColumn("county",            F.coalesce(F.col("_ref_county"), F.col("county")))
        .drop("_join_code", "_ref_code", "_ref_name", "_ref_county")
    )
    return enriched
