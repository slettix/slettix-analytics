"""
02_sla_monitor — Airflow DAG for SLA-overvåking av dataprodukter.

Kjører hvert 30. minutt og sjekker om hvert dataprodukt med en
`quality_sla.freshness_hours`-definisjon er oppdatert i tide.

Resultat lagres til s3://gold/sla_results/<product_id>/latest.json
og er tilgjengelig via GET /api/products/{id}/sla i portalen.

PERF-4: etter SLA-sjekk beregnes også 30-dagers compliance-aggregat
(`sla_compliance_30d`) og PATCH-es til produktmanifestet, slik at
portalen kan vise verdien uten å lese 1440 MinIO-objekter per visning.
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/jobs")  # hostPath: jobs/ montert i Helm values

_MINIO_ENDPOINT = "http://minio.slettix-analytics.svc.cluster.local:9000"

default_args = {
    "owner":            "slettix",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=5),
}


def _compute_compliance_30d(s3, log, product_id: str, days: int = 30) -> float | None:
    """Les SLA-historikk siste N dager og returner compliance i prosent.

    Bruker key-prefix-filter på timestamp i filnavn (`<YYYYMMDDTHHMMSS>.json`)
    for å unngå å hente objekter som er eldre enn N dager.
    """
    try:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)
        cutoff_key_prefix = cutoff.strftime("%Y%m%dT%H%M%S")
        prefix = f"sla_results/{product_id}/history/"

        keys: list[str] = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket="gold", Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                # Filename: <prefix><ts>.json — kun keys nyere enn cutoff
                fname = obj["Key"][len(prefix):]
                ts_part = fname.replace(".json", "")
                if ts_part >= cutoff_key_prefix:
                    keys.append(obj["Key"])

        if not keys:
            return None

        compliant_count = 0
        total = 0
        for key in keys:
            try:
                obj = s3.get_object(Bucket="gold", Key=key)
                entry = json.loads(obj["Body"].read())
                total += 1
                if entry.get("compliant") is True:
                    compliant_count += 1
            except Exception as exc:
                log.warning(f"Hopper over {key}: {exc}")

        if total == 0:
            return None
        return round(compliant_count / total * 100, 1)
    except Exception as exc:
        log.warning(f"Kunne ikke beregne 30d-compliance for {product_id}: {exc}")
        return None


def _patch_manifest(log, product_id: str, payload: dict) -> None:
    """PATCH /api/products/{id} med precomputed-felter. Best-effort — feiler stille."""
    import requests

    portal_url = os.environ.get("PORTAL_URL", "http://dataportal.slettix-analytics.svc.cluster.local:8090")
    api_key    = os.environ.get("PORTAL_API_KEY", "dev-key-change-me")
    try:
        resp = requests.patch(
            f"{portal_url}/api/products/{product_id}",
            json=payload,
            headers={"X-API-Key": api_key},
            timeout=10,
        )
        if resp.ok:
            log.info(f"  ✓ {product_id}: PATCH-et compliance til portal")
        else:
            log.warning(f"  ✗ {product_id}: portal PATCH {resp.status_code}: {resp.text[:200]}")
    except Exception as exc:
        log.warning(f"  ✗ {product_id}: PATCH feilet: {exc}")


def check_sla(**context):
    """
    For hvert produkt med freshness_hours-SLA:
      1. Les siste commit-tidspunkt fra Delta-tabellen
      2. Beregn timer siden siste oppdatering
      3. Avgjør om SLA er overholdt
      4. Lagre resultat til s3://gold/sla_results/<product_id>/latest.json
      5. Beregn 30d-compliance-aggregat og PATCH til portal-manifest (PERF-4)
    """
    import boto3
    from botocore.client import Config
    from deltalake import DeltaTable
    from registry import list_all

    log = context["task_instance"].log

    minio_endpoint   = os.environ.get("MINIO_ENDPOINT", _MINIO_ENDPOINT)
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "admin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "changeme")

    storage_options = {
        "AWS_ENDPOINT_URL":           minio_endpoint,
        "AWS_ACCESS_KEY_ID":          minio_access_key,
        "AWS_SECRET_ACCESS_KEY":      minio_secret_key,
        "AWS_ALLOW_HTTP":             "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_EC2_METADATA_DISABLED":  "true",
    }

    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=Config(signature_version="s3v4"),
    )

    products  = list_all()
    now       = datetime.now(tz=timezone.utc)
    checked   = 0
    breached  = 0

    for product in products:
        sla = product.get("quality_sla", {})
        freshness_hours = sla.get("freshness_hours")
        if not freshness_hours:
            continue

        product_id  = product["id"]
        source_path = product["source_path"]
        log.info(f"Sjekker SLA for {product_id} ({freshness_hours}t ferskhet) ...")

        last_updated    = None
        hours_since     = None
        compliant       = None
        error           = None

        try:
            dt      = DeltaTable(source_path, storage_options=storage_options)
            history = dt.history(limit=1)
            if history:
                ts_ms        = history[0].get("timestamp")
                last_updated = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                hours_since  = (now - last_updated).total_seconds() / 3600
                compliant    = hours_since <= freshness_hours
            else:
                compliant = False
                error     = "Ingen commit-historikk i Delta-tabellen"
        except Exception as exc:
            compliant = False
            error     = str(exc)
            log.warning(f"Kunne ikke lese Delta-tabell for {product_id}: {exc}")

        result = {
            "product_id":         product_id,
            "checked_at":         now.isoformat(),
            "compliant":          compliant,
            "last_updated":       last_updated.isoformat() if last_updated else None,
            "hours_since_update": round(hours_since, 2) if hours_since is not None else None,
            "freshness_hours":    freshness_hours,
        }
        if error:
            result["error"] = error

        if not compliant:
            breached += 1
            log.warning(
                json.dumps({
                    "event":       "sla_breach",
                    "product_id":  product_id,
                    "hours_since": round(hours_since, 2) if hours_since else None,
                    "sla_hours":   freshness_hours,
                })
            )
        else:
            log.info(f"  ✓ {product_id}: {hours_since:.1f}t siden oppdatering (SLA: {freshness_hours}t)")

        ts_key = now.strftime("%Y%m%dT%H%M%S")
        try:
            body = json.dumps(result, indent=2).encode()
            s3.put_object(
                Bucket="gold",
                Key=f"sla_results/{product_id}/latest.json",
                Body=body,
                ContentType="application/json",
            )
            s3.put_object(
                Bucket="gold",
                Key=f"sla_results/{product_id}/history/{ts_key}.json",
                Body=body,
                ContentType="application/json",
            )
        except Exception as exc:
            log.warning(f"Kunne ikke lagre SLA-resultat for {product_id}: {exc}")

        checked += 1

        # PERF-4: beregn 30d-compliance og PATCH til portal-manifest. Best-effort.
        compliance_30d = _compute_compliance_30d(s3, log, product_id, days=30)
        if compliance_30d is not None:
            _patch_manifest(log, product_id, {
                "sla_compliance_30d":          compliance_30d,
                "sla_aggregate_computed_at":   now.isoformat(),
            })

    log.info(f"SLA-sjekk fullført: {checked} produkter, {breached} brudd.")


with DAG(
    dag_id="02_sla_monitor",
    description="Overvåker SLA-ferskhet for alle registrerte dataprodukter",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["sla", "monitor", "data-mesh"],
) as dag:

    PythonOperator(
        task_id="check_sla",
        python_callable=check_sla,
    )
