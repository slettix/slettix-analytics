"""
helse_dar_validate_quality — Spark-jobb som validerer Gold-tabellene
for dødsårsaksregisteret etter at de er bygd.

Sjekker:
  * helse.dar.dodsfall_enriched
      - dodsfall_id not null
      - underliggende_arsak_kode not null
      - underliggende_arsak_kapittel not null  (verifiserer ICD-10-join virker)
      - row_count >= 1
  * helse.dar.medvirkende_arsaker
      - dodsfall_id not null
      - icd10_kode not null
      - row_count >= 0  (kan være tom hvis ingen dødsfall har medvirkende)

Resultater skrives til:
  s3://gold/quality_results/<product_id>/latest.json
  s3://gold/quality_results/<product_id>/history/<ts>.json

Følger samme mønster som folkeregister_validate_quality.py — én Spark-jobb
som dekker hele Gold-laget i domenet.
"""

import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _write_json_to_s3a(spark: SparkSession, key: str, body: bytes) -> None:
    """Skriv enkeltfil til s3a://gold/<key> via Hadoop FileSystem API.

    Bruker samme S3A-konfigurasjon som DataFrame-leseren, så vi unngår
    avhengighet til boto3 i Spark-imaget.
    """
    sc   = spark.sparkContext
    jvm  = sc._gateway.jvm
    URI  = jvm.java.net.URI
    Path = jvm.org.apache.hadoop.fs.Path
    fs   = jvm.org.apache.hadoop.fs.FileSystem.get(
        URI("s3a://gold"), sc._jsc.hadoopConfiguration()
    )
    out = fs.create(Path(f"s3a://gold/{key}"), True)
    try:
        out.write(body)
    finally:
        out.close()


CHECKS = [
    {
        "product_id": "helse.dar.dodsfall_enriched",
        "path":       "s3a://gold/helse/dar/dodsfall_enriched",
        "expectations": [
            ("expect_column_values_to_not_be_null",  {"column": "dodsfall_id"}),
            ("expect_column_values_to_not_be_null",  {"column": "underliggende_arsak_kode"}),
            ("expect_column_values_to_not_be_null",  {"column": "underliggende_arsak_kapittel"}),
            ("expect_table_row_count_to_be_between", {"min_value": 1, "max_value": None}),
        ],
    },
    {
        "product_id": "helse.dar.medvirkende_arsaker",
        "path":       "s3a://gold/helse/dar/medvirkende_arsaker",
        "expectations": [
            ("expect_column_values_to_not_be_null",  {"column": "dodsfall_id"}),
            ("expect_column_values_to_not_be_null",  {"column": "icd10_kode"}),
            ("expect_table_row_count_to_be_between", {"min_value": 0, "max_value": None}),
        ],
    },
]


def _evaluate(df, exp_type: str, kwargs: dict, total_rows: int) -> tuple[bool, str | None]:
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
        return (True, None) if col in df.columns else (False, f"Kolonne '{col}' eksisterer ikke")
    if exp_type == "expect_table_row_count_to_be_between":
        mn = kwargs.get("min_value")
        mx = kwargs.get("max_value")
        if mn is not None and total_rows < mn:
            return False, f"Radantall {total_rows} < min_value {mn}"
        if mx is not None and total_rows > mx:
            return False, f"Radantall {total_rows} > max_value {mx}"
        return True, None
    return False, f"Ukjent expectation-type: {exp_type}"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("helse_dar_validate_quality")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    overall_failed = 0

    for check in CHECKS:
        product_id = check["product_id"]
        print(f"Validerer {product_id} ...")

        try:
            df         = spark.read.format("delta").load(check["path"])
            total_rows = df.count()
            results    = []
            for exp_type, kwargs in check["expectations"]:
                success, error = _evaluate(df, exp_type, kwargs, total_rows)
                results.append({
                    "expectation": exp_type,
                    "kwargs":      kwargs,
                    "success":     success,
                    "error":       error,
                })

            passed   = sum(1 for r in results if r["success"])
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
        body   = json.dumps(quality_result, indent=2).encode()
        for key in [
            f"quality_results/{product_id}/latest.json",
            f"quality_results/{product_id}/history/{ts_key}.json",
        ]:
            try:
                _write_json_to_s3a(spark, key, body)
            except Exception as exc:
                print(f"Kunne ikke lagre kvalitetsresultat for {product_id}: {exc}")

        print(
            f"{product_id}: {quality_result['score_pct']}% "
            f"({quality_result['passed']}/{quality_result['total_expectations']}) "
            f"rows={quality_result['row_count']}"
        )

    spark.stop()

    if overall_failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
