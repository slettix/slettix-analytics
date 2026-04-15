"""
governance_retention — daglig sletting av data eldre enn retensjonsgrensen.

For hvert dataprodukt med retention-policy:
  1. Les manifest fra portalen
  2. Finn rader eldre enn retention.days i Delta-tabellen
  3. Slett ved hjelp av deltalake.write_deltalake (overwrite av filtrert tabell)
  4. Kjør VACUUM for å frigjøre diskplass
"""

import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "changeme")
PORTAL_URL       = os.environ.get("PORTAL_URL",       "http://dataportal:8090")
PORTAL_API_KEY   = os.environ.get("PORTAL_API_KEY",   "dev-key-change-me")

_STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL":           MINIO_ENDPOINT,
    "AWS_ACCESS_KEY_ID":          MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY":      MINIO_SECRET_KEY,
    "AWS_ALLOW_HTTP":             "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

default_args = {
    "owner": "slettix",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _apply_retention(**context):
    import requests
    import pandas as pd
    from deltalake import DeltaTable, write_deltalake

    headers = {"X-API-Key": PORTAL_API_KEY}
    resp    = requests.get(f"{PORTAL_URL}/api/products", headers=headers, timeout=10)
    resp.raise_for_status()
    products = resp.json()

    now         = datetime.now(tz=timezone.utc)
    total_deleted = 0

    for product in products:
        manifest  = product.get("manifest", product)
        retention = manifest.get("retention")
        if not retention:
            continue

        days       = retention.get("days")
        strategy   = retention.get("strategy", "delete")
        part_col   = retention.get("partition_column")
        source_path = manifest.get("source_path", "")

        if not days or not source_path or strategy != "delete":
            continue

        cutoff = now - timedelta(days=days)
        print(f"[retention] {manifest['id']}: sletler data eldre enn {cutoff.date()} ({days} dager)")

        try:
            dt = DeltaTable(source_path, storage_options=_STORAGE_OPTIONS)
            df = dt.to_pandas()

            if part_col and part_col in df.columns:
                col = pd.to_datetime(df[part_col], errors="coerce", utc=True)
                mask = col >= cutoff
            else:
                # Fallback: bruk ingen filtrering hvis ingen partisjoneringskolonne
                print(f"[retention] {manifest['id']}: ingen partition_column, hopper over")
                continue

            before = len(df)
            df_kept = df[mask].reset_index(drop=True)
            deleted = before - len(df_kept)

            if deleted > 0:
                import pyarrow as pa
                arrow_table = pa.Table.from_pandas(df_kept, preserve_index=False)
                write_deltalake(source_path, arrow_table, mode="overwrite",
                                storage_options=_STORAGE_OPTIONS)
                dt2 = DeltaTable(source_path, storage_options=_STORAGE_OPTIONS)
                dt2.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
                print(f"[retention] {manifest['id']}: slettet {deleted} rader, beholdt {len(df_kept)}")
                total_deleted += deleted
            else:
                print(f"[retention] {manifest['id']}: ingen rader å slette")

        except Exception as exc:
            print(f"[retention] FEIL for {manifest.get('id', '?')}: {exc}")

    print(f"[retention] Ferdig. Totalt slettet: {total_deleted} rader.")


with DAG(
    dag_id="governance_retention",
    description="Daglig sletting av data i henhold til retensjons-policy",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["governance", "gdpr", "retention"],
) as dag:

    apply_retention = PythonOperator(
        task_id="apply_retention",
        python_callable=_apply_retention,
    )
