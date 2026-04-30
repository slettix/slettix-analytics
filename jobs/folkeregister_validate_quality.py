"""
Datakvalitetsvalidering for Folkeregister Silver-tabellene.

Erstatter pandas-baserte `validate_quality` (kjørte på scheduler-pod og OOM-killet
ved person_registry > 264 MB). Spark-jobben skalerer ubegrenset med executor-
ressurser, og resultater skrives på samme JSON-format til
`s3://gold/quality_results/<product_id>/`.

Sjekker:
  expect_column_values_to_not_be_null
  expect_column_to_exist
  expect_table_row_count_to_be_between

Kjøring:
  spark-submit /opt/spark/jobs/folkeregister_validate_quality.py
"""

import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _write_json_to_s3a(spark, key: str, body: bytes) -> None:
    """Skriv enkeltfil til s3a://gold/<key> via Hadoop FileSystem API.

    Unngår boto3-avhengighet i Spark-imaget — bruker samme konfigurasjon
    (S3A + credentials) som DataFrame-leseren.
    """
    sc   = spark.sparkContext
    jvm  = sc._gateway.jvm
    URI  = jvm.java.net.URI
    Path = jvm.org.apache.hadoop.fs.Path
    fs   = jvm.org.apache.hadoop.fs.FileSystem.get(
        URI("s3a://gold"), sc._jsc.hadoopConfiguration()
    )
    out  = fs.create(Path(f"s3a://gold/{key}"), True)
    try:
        out.write(body)
    finally:
        out.close()


CHECKS = [
    {
        "product_id": "folkeregister.person_registry",
        "path":       "s3a://silver/folkeregister/person_registry",
        "expectations": [
            ("expect_column_values_to_not_be_null", {"column": "person_id"}),
            ("expect_column_values_to_not_be_null", {"column": "full_name"}),
            ("expect_column_values_to_not_be_null", {"column": "fnr_masked"}),
            ("expect_column_to_exist",              {"column": "marital_status"}),
            ("expect_table_row_count_to_be_between", {"min_value": 1, "max_value": None}),
        ],
    },
    {
        "product_id": "folkeregister.family_relations",
        "path":       "s3a://silver/folkeregister/family_relations",
        "expectations": [
            ("expect_column_values_to_not_be_null", {"column": "person_id_a"}),
            ("expect_column_values_to_not_be_null", {"column": "person_id_b"}),
            ("expect_table_row_count_to_be_between", {"min_value": 1, "max_value": None}),
        ],
    },
    {
        "product_id": "folkeregister.residence_history",
        "path":       "s3a://silver/folkeregister/residence_history",
        "expectations": [
            ("expect_column_values_to_not_be_null", {"column": "person_id"}),
            ("expect_table_row_count_to_be_between", {"min_value": 1, "max_value": None}),
        ],
    },
]


def _evaluate_expectation(df, exp_type: str, kwargs: dict, total_rows: int) -> tuple[bool, str | None]:
    """Returnerer (success, error_message). Spark-tellingene kjøres lazy."""
    if exp_type == "expect_column_values_to_not_be_null":
        col = kwargs["column"]
        if col not in df.columns:
            return False, f"Kolonne '{col}' eksisterer ikke"
        nulls = df.filter(F.col(col).isNull()).count()
        if nulls > 0:
            return False, f"{nulls} null-verdier i kolonne '{col}'"
        return True, None

    if exp_type == "expect_column_to_exist":
        col = kwargs["column"]
        if col in df.columns:
            return True, None
        return False, f"Kolonne '{col}' eksisterer ikke"

    if exp_type == "expect_table_row_count_to_be_between":
        min_value = kwargs.get("min_value")
        max_value = kwargs.get("max_value")
        if min_value is not None and total_rows < min_value:
            return False, f"Radantall {total_rows} < min_value {min_value}"
        if max_value is not None and total_rows > max_value:
            return False, f"Radantall {total_rows} > max_value {max_value}"
        return True, None

    return False, f"Ukjent expectation-type: {exp_type}"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("folkeregister_validate_quality")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    overall_failed = 0

    for check in CHECKS:
        product_id = check["product_id"]
        print(f"Validerer {product_id} ...")

        try:
            df = spark.read.format("delta").load(check["path"])
            total_rows = df.count()

            results = []
            for exp_type, kwargs in check["expectations"]:
                success, error = _evaluate_expectation(df, exp_type, kwargs, total_rows)
                results.append({
                    "expectation": exp_type,
                    "kwargs":      kwargs,
                    "success":     success,
                    "error":       error,
                })

            passed = sum(1 for r in results if r["success"])
            failures = [
                {"expectation": r["expectation"], "kwargs": r["kwargs"], "error": r["error"]}
                for r in results if not r["success"]
            ]
            quality_result = {
                "product_id":         product_id,
                "validated_at":       datetime.now(tz=timezone.utc).isoformat(),
                "score_pct":          round(passed / len(results) * 100, 1),
                "total_expectations": len(results),
                "passed":             passed,
                "failed":             len(failures),
                "row_count":          total_rows,
                "failures":           failures,
            }
            if failures:
                overall_failed += 1
        except Exception as exc:
            print(f"Validering av {product_id} feilet: {exc}")
            quality_result = {
                "product_id":         product_id,
                "validated_at":       datetime.now(tz=timezone.utc).isoformat(),
                "score_pct":          0.0,
                "total_expectations": 0,
                "passed":             0,
                "failed":             1,
                "row_count":          None,
                "failures":           [{"expectation": "pipeline", "error": str(exc)}],
            }
            overall_failed += 1

        ts_key = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
        body = json.dumps(quality_result, indent=2).encode()
        for key in [
            f"quality_results/{product_id}/latest.json",
            f"quality_results/{product_id}/history/{ts_key}.json",
        ]:
            try:
                _write_json_to_s3a(spark, key, body)
            except Exception as exc:
                print(f"Kunne ikke lagre kvalitetsresultat for {product_id}: {exc}")

        print(f"{product_id}: {quality_result['score_pct']}% ({quality_result['passed']}/{quality_result['total_expectations']})  rows={quality_result['row_count']}")

    spark.stop()

    if overall_failed > 0:
        # Exit 1 så Airflow markerer task som failed
        raise SystemExit(1)


if __name__ == "__main__":
    main()
