"""
silver_validate_quality — generisk datakvalitetscheck for Silver-tabeller
generert av silver_transform-malen (silver-wizard).

Kjører som SparkApplication etter bronze_to_silver. Leser samme config-fil
(s3a://config/silver/<slug>/current.json) som transform-jobben, og sjekker:

  • row_count > 0                                              (kritisk)
  • primary_key er ikke-null på alle rader                     (kritisk)
  • primary_key er unik (ingen duplikater)                     (kritisk)
  • drop_if_null-kolonner har faktisk 0 null-rader              (kritisk)
  • payload-extract-felter har null-rate ≤ 50%                  (warning)

Resultatet skrives til
  s3a://gold/quality_results/<product_id>/latest.json
  s3a://gold/quality_results/<product_id>/history/<ts>.json

Ved kritisk feil exit-er jobben non-zero så Airflow markerer task-en failed.
Warnings logges men feiler ikke task-en (jf. helse_dar_validate_quality-mønsteret
i b4cbc6a som skiller warning vs critical).

Bruk:
  spark-submit /opt/spark/jobs/silver_validate_quality.py \\
    --config s3a://config/silver/<slug>/current.json \\
    --product-id helse.kreftregisteret
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


log = logging.getLogger("silver_validate_quality")
logging.basicConfig(level=logging.INFO, format="%(message)s")


def _load_config(spark, path: str) -> dict:
    """Samme S3A-baserte config-lasting som transform_bronze_to_silver.py."""
    if path.startswith(("s3://", "s3a://")):
        if path.startswith("s3://"):
            path = "s3a://" + path[len("s3://"):]
        content = spark.read.text(path, wholetext=True).collect()[0][0]
        data = json.loads(content)
    else:
        data = json.loads(Path(path).read_text())
    if isinstance(data, dict) and "config" in data and isinstance(data["config"], dict):
        return data["config"]
    return data


def _normalize_s3_path(path: str) -> str:
    if isinstance(path, str) and path.startswith("s3://"):
        return "s3a://" + path[len("s3://"):]
    return path


def _write_result(spark, product_id: str, result: dict) -> None:
    """Skriv result-JSON til gold/quality_results via Hadoop FileSystem.

    Bruker JVM FileSystem direkte (ikke spark.write.text — som ville skrive
    en partisjonert mappe, ikke en enkelt fil). En enkelt JSON-fil per kjøring
    er nødvendig for at portalen kan lese latest.json som ett objekt.
    """
    sc       = spark.sparkContext
    hconf    = sc._jsc.hadoopConfiguration()
    FS       = sc._jvm.org.apache.hadoop.fs.FileSystem
    HPath    = sc._jvm.org.apache.hadoop.fs.Path
    URI      = sc._jvm.java.net.URI

    payload  = json.dumps(result, indent=2, ensure_ascii=False).encode("utf-8")
    ts_key   = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")

    for key in (
        f"quality_results/{product_id}/latest.json",
        f"quality_results/{product_id}/history/{ts_key}.json",
    ):
        uri = f"s3a://gold/{key}"
        fs  = FS.get(URI(uri), hconf)
        out = fs.create(HPath(uri), True)  # overwrite=true
        out.write(payload)
        out.close()


def run_checks(spark: SparkSession, config: dict, product_id: str) -> dict:
    target          = _normalize_s3_path(config["target"])
    primary_key     = config["primary_key"]
    null_rules      = config.get("null_handling", {}) or {}
    drop_if_null    = list(null_rules.get("drop_if_null") or [])
    payload_extract = config.get("payload_extract") or {}
    payload_fields  = list((payload_extract.get("fields") or {}).keys())

    log.info(json.dumps({"event": "quality_start", "product_id": product_id, "target": target}))

    df         = spark.read.format("delta").load(target)
    row_count  = df.count()

    checks_critical: list[dict] = []
    checks_warning:  list[dict] = []

    # 1) row_count > 0
    checks_critical.append({
        "name":     "row_count_nonzero",
        "passed":   row_count > 0,
        "observed": row_count,
        "expected": "> 0",
    })

    if row_count > 0:
        # 2) primary_key ikke-null
        pk_null = df.filter(F.col(primary_key).isNull()).count()
        checks_critical.append({
            "name":     f"primary_key_not_null[{primary_key}]",
            "passed":   pk_null == 0,
            "observed": pk_null,
            "expected": 0,
        })

        # 3) primary_key unik
        pk_distinct = df.select(primary_key).distinct().count()
        checks_critical.append({
            "name":     f"primary_key_unique[{primary_key}]",
            "passed":   pk_distinct == row_count,
            "observed": {"distinct": pk_distinct, "total": row_count},
            "expected": "distinct == total",
        })

        # 4) drop_if_null-kolonner skal ha 0 null
        for col in drop_if_null:
            if col not in df.columns:
                checks_critical.append({
                    "name":     f"drop_if_null_column_exists[{col}]",
                    "passed":   False,
                    "observed": "missing",
                    "expected": "present",
                })
                continue
            nulls = df.filter(F.col(col).isNull()).count()
            checks_critical.append({
                "name":     f"no_nulls_in_required[{col}]",
                "passed":   nulls == 0,
                "observed": nulls,
                "expected": 0,
            })

        # 5) payload-felter — null-rate ≤ 50% (warning)
        for col in payload_fields:
            if col not in df.columns or col in drop_if_null:
                continue  # allerede sjekket, eller ikke til stede
            nulls = df.filter(F.col(col).isNull()).count()
            rate  = nulls / row_count
            checks_warning.append({
                "name":     f"payload_null_rate_under_50pct[{col}]",
                "passed":   rate <= 0.50,
                "observed": round(rate, 4),
                "expected": "<= 0.5",
            })

    critical_failures = [c for c in checks_critical if not c["passed"]]
    warning_failures  = [c for c in checks_warning  if not c["passed"]]

    # Bygg portal-kompatible felter (samme skjema som det eldre quality-systemet
    # for f.eks. helse.dar.dodsfall_enriched). Portalen leser score_pct,
    # passed, total_expectations, failures direkte fra latest.json — uten disse
    # feltene kræsjer catalog.html-templatet på `quality_score >= 80`.
    all_checks = checks_critical + checks_warning
    total      = len(all_checks)
    passed     = sum(1 for c in all_checks if c["passed"])
    failed     = total - passed
    score_pct  = round(passed / total * 100, 1) if total else 100.0
    now_iso    = datetime.now(tz=timezone.utc).isoformat()

    failures_legacy = [
        {
            "expectation": c["name"],
            "kwargs":      {},  # vi har ikke separate kwargs — info ligger i name
            "error":       f"observed={c['observed']}, expected={c['expected']}",
        }
        for c in all_checks if not c["passed"]
    ]

    return {
        # Portal-kompatible felter
        "product_id":         product_id,
        "validated_at":       now_iso,
        "score_pct":          score_pct,
        "total_expectations": total,
        "passed":             passed,
        "failed":             failed,
        "row_count":          row_count,
        "failures":           failures_legacy,
        # Detaljerte felter (vår nye jobb)
        "checked_at":         now_iso,
        "target":             target,
        "checks_critical":    checks_critical,
        "checks_warning":     checks_warning,
        "critical_failures":  len(critical_failures),
        "warning_failures":   len(warning_failures),
        "status":             "failed" if critical_failures else ("warning" if warning_failures else "ok"),
    }


def main():
    parser = argparse.ArgumentParser(description="Silver datakvalitet-validering")
    parser.add_argument("--config",     required=True, help="Path til silver config JSON (samme som transform-jobben)")
    parser.add_argument("--product-id", required=True, help="Dataprodukt-id (brukes som mappenavn i quality_results)")
    args = parser.parse_args()

    config_slug = Path(args.config).parent.name or "config"
    spark = (
        SparkSession.builder
        .appName(f"silver_validate_quality:{config_slug}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    config = _load_config(spark, args.config)
    t0     = time.time()
    result = run_checks(spark, config, args.product_id)
    result["elapsed_s"] = round(time.time() - t0, 2)

    _write_result(spark, args.product_id, result)
    log.info(json.dumps({"event": "quality_end", **{k: v for k, v in result.items() if k not in ("checks_critical", "checks_warning")}}))

    spark.stop()

    # Exit non-zero ved kritiske feil så Airflow markerer task-en failed.
    if result["status"] == "failed":
        log.error(json.dumps({"event": "quality_critical_failures", "failures": [
            c for c in result["checks_critical"] if not c["passed"]
        ]}))
        sys.exit(2)


if __name__ == "__main__":
    main()
