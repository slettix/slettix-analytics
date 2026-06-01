"""
pipeline_stats — kort hjelpefunksjon for å logge pipeline-statistikk fra
Spark-jobber. Skriver et lite JSON-objekt til
s3a://gold/pipeline_stats/<product_id>/latest.json + history/<ts>.json.

Best-effort: feiler aldri jobben. Brukes av folkeregister_residence_history
og kan tas inn i flere Spark-jobber etterhvert.
"""

import json
from datetime import datetime, timezone


def report_stats(spark, product_id: str, target_path: str, row_count: int) -> None:
    """Skriv pipeline-stats til MinIO via Hadoop S3A som Spark allerede har
    konfigurert. Stille feilmodus — log warning og fortsett."""
    now    = datetime.now(tz=timezone.utc)
    record = {
        "product_id":    product_id,
        "target_path":   target_path,
        "row_count":     int(row_count),
        "reported_at":   now.isoformat(),
    }
    payload = json.dumps(record, ensure_ascii=False, indent=2).encode("utf-8")
    ts_key  = now.strftime("%Y%m%dT%H%M%S")

    try:
        sc        = spark.sparkContext
        hconf     = sc._jsc.hadoopConfiguration()
        FS        = sc._jvm.org.apache.hadoop.fs.FileSystem
        HPath     = sc._jvm.org.apache.hadoop.fs.Path
        URI       = sc._jvm.java.net.URI
        for key in (
            f"pipeline_stats/{product_id}/latest.json",
            f"pipeline_stats/{product_id}/history/{ts_key}.json",
        ):
            uri = f"s3a://gold/{key}"
            fs  = FS.get(URI(uri), hconf)
            out = fs.create(HPath(uri), True)  # overwrite=true
            out.write(payload)
            out.close()
    except Exception as exc:
        try:
            spark.sparkContext._jvm.org.apache.log4j.LogManager \
                .getLogger("pipeline_stats") \
                .warn(f"report_stats failed for {product_id}: {exc}")
        except Exception:
            pass
