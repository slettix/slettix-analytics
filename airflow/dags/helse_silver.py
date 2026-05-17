"""
helse_silver — Airflow DAG for helse-domenets Silver-laget.

Per nå én task:
  • load_icd10_reference — leser ICD-10-kodeverket fra
    s3://config/reference/icd10-2026.csv, beriker hierarki
    (chapter_code/block_code) og skriver Delta-tabellen
    s3://silver/helse/icd10. Dataprodukt: helse.icd10.

Generelt mønster: referansedata for kodeverk. Kildefilen ligger i MinIO
(ikke ConfigMap) fordi den er ~1.4 MB — ConfigMap-grensen er 1 MiB per item
og 256 KB for `last-applied-configuration`-annotasjonen.
"""

import io
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/jobs")

ICD10_SOURCE_BUCKET = "config"
ICD10_SOURCE_KEY    = "reference/icd10-2026.csv"
ICD10_TARGET        = "s3://silver/helse/icd10"
ICD10_VERSION       = "2026"


def _storage_options() -> dict:
    return {
        "AWS_ENDPOINT_URL":           os.environ.get("MINIO_ENDPOINT",   "http://minio.slettix-analytics.svc.cluster.local:9000"),
        "AWS_ACCESS_KEY_ID":          os.environ.get("MINIO_ACCESS_KEY", "admin"),
        "AWS_SECRET_ACCESS_KEY":      os.environ.get("MINIO_SECRET_KEY", "changeme"),
        "AWS_ALLOW_HTTP":             "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def _enrich_with_hierarchy(df):
    """Utled chapter_code og block_code for hver rad ved å traversere parent-
    kjeden oppover. KAPITTEL får chapter_code=code, block_code=NULL. BLOKK
    får chapter_code=parent, block_code=NULL. KODE og dypere får begge."""
    # Bygg parent→record-lookup for rask traversering
    by_id = {row["identifier"]: (row["type"], row["parent"]) for _, row in df.iterrows()}

    def traverse(identifier: str) -> tuple[str | None, str | None]:
        chapter, block = None, None
        cur = identifier
        seen: set[str] = set()
        while cur and cur not in seen:
            seen.add(cur)
            rec = by_id.get(cur)
            if not rec:
                break
            type_, parent = rec
            if type_ == "BLOKK" and block is None:
                block = cur
            elif type_ == "KAPITTEL":
                chapter = cur
                break
            cur = parent
        return chapter, block

    chapter_codes: list[str | None] = []
    block_codes:   list[str | None] = []
    for _, row in df.iterrows():
        if row["type"] == "KAPITTEL":
            chapter_codes.append(row["identifier"])
            block_codes.append(None)
        elif row["type"] == "BLOKK":
            chapter_codes.append(row["parent"])
            block_codes.append(None)
        else:  # KODE eller dypere
            ch, bl = traverse(row["identifier"])
            chapter_codes.append(ch)
            block_codes.append(bl)
    df["chapter_code"] = chapter_codes
    df["block_code"]   = block_codes
    return df


def load_icd10_reference(**context):
    """Leser ICD-10-kodeverket og skriver beriket Delta-tabell til
    s3://silver/helse/icd10. Overskriver hele tabellen ved hver kjøring
    (referansedata, ikke transaksjoner)."""
    import boto3
    import pandas as pd
    import pyarrow as pa
    from botocore.client import Config as BotoConfig
    from deltalake.writer import write_deltalake

    log = context["task_instance"].log

    minio_endpoint   = os.environ.get("MINIO_ENDPOINT",   "http://minio.slettix-analytics.svc.cluster.local:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "admin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "changeme")

    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=BotoConfig(signature_version="s3v4"),
    )
    obj = s3.get_object(Bucket=ICD10_SOURCE_BUCKET, Key=ICD10_SOURCE_KEY)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()), dtype=str)

    log.info(f"Lest {len(df)} rader fra s3://{ICD10_SOURCE_BUCKET}/{ICD10_SOURCE_KEY}")

    # Beriker med hierarki-koder
    df = _enrich_with_hierarchy(df)

    # Bygge endelig skjema: rename + casting + metadata
    df = df.rename(columns={
        "identifier":           "code",
        "parent":               "parent_code",
        "tittel_no":            "name_no",
        "tittel_en":            "name_en",
        "gyldig_underliggende": "valid_as_underlying",
        "gyldig_ekstern":       "valid_as_external",
    })
    df["valid_as_underlying"] = df["valid_as_underlying"].fillna("false").str.lower().map({"true": True, "false": False})
    df["valid_as_external"]   = df["valid_as_external"].fillna("false").str.lower().map({"true": True, "false": False})
    df["icd10_version"]       = ICD10_VERSION
    df["_ingested_at"]        = datetime.now(tz=timezone.utc)

    # Reorder kolonner for lesbarhet
    df = df[[
        "code", "parent_code", "type", "name_no", "name_en",
        "chapter_code", "block_code",
        "valid_as_underlying", "valid_as_external",
        "icd10_version", "_ingested_at",
    ]]

    # Eksplisitt pyarrow-skjema — uten dette får string-kolonner som er
    # null-only (typisk parent_code/name_en for nye datasett) typen `null`
    # som Delta Lake avviser.
    schema = pa.schema([
        ("code",                pa.string()),
        ("parent_code",         pa.string()),
        ("type",                pa.string()),
        ("name_no",             pa.string()),
        ("name_en",             pa.string()),
        ("chapter_code",        pa.string()),
        ("block_code",          pa.string()),
        ("valid_as_underlying", pa.bool_()),
        ("valid_as_external",   pa.bool_()),
        ("icd10_version",       pa.string()),
        ("_ingested_at",        pa.timestamp("us", tz="UTC")),
    ])
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

    write_deltalake(
        ICD10_TARGET,
        table,
        mode="overwrite",
        storage_options=_storage_options(),
    )
    log.info(f"Skrev {len(df)} ICD-10-rader til {ICD10_TARGET} (version={ICD10_VERSION})")

    # Telle nivåer for sanity-check
    counts = df["type"].value_counts().to_dict()
    log.info(f"Fordeling: {counts}")


default_args = {
    "owner":                     "helse-team",
    "retries":                   1,
    "retry_delay":               timedelta(minutes=2),
    "email_on_failure":          False,
}


with DAG(
    dag_id="helse_silver",
    description="Helse-domenets Silver-lag: referansedata og kodeverk (per nå: ICD-10)",
    schedule="@weekly",
    start_date=datetime(2024, 1, 15),
    catchup=False,
    default_args=default_args,
    tags=["helse", "silver", "reference", "kodeverk"],
    doc_md=__doc__,
) as dag:

    load_icd10 = PythonOperator(
        task_id="load_icd10_reference",
        python_callable=load_icd10_reference,
        execution_timeout=timedelta(minutes=10),
    )
