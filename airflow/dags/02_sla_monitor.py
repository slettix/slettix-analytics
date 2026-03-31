"""
02_sla_monitor — Airflow DAG for SLA-overvåking av dataprodukter.

Kjører hvert 30. minutt og sjekker om hvert dataprodukt med en
`quality_sla.freshness_hours`-definisjon er oppdatert i tide.

Resultat lagres til s3://gold/sla_results/<product_id>/latest.json
og er tilgjengelig via GET /api/products/{id}/sla i portalen.
"""

import json
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/spark_jobs")

default_args = {
    "owner":            "slettix",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=5),
}


def check_sla(**context):
    """
    For hvert produkt med freshness_hours-SLA:
      1. Les siste commit-tidspunkt fra Delta-tabellen
      2. Beregn timer siden siste oppdatering
      3. Avgjør om SLA er overholdt
      4. Lagre resultat til s3://gold/sla_results/<product_id>/latest.json
    """
    import boto3
    from botocore.client import Config
    from deltalake import DeltaTable
    from registry import list_all

    log = context["task_instance"].log

    storage_options = {
        "AWS_ENDPOINT_URL":           "http://minio:9000",
        "AWS_ACCESS_KEY_ID":          "admin",
        "AWS_SECRET_ACCESS_KEY":      "changeme",
        "AWS_ALLOW_HTTP":             "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_EC2_METADATA_DISABLED":  "true",
    }

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="changeme",
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

        try:
            s3.put_object(
                Bucket="gold",
                Key=f"sla_results/{product_id}/latest.json",
                Body=json.dumps(result, indent=2).encode(),
                ContentType="application/json",
            )
        except Exception as exc:
            log.warning(f"Kunne ikke lagre SLA-resultat for {product_id}: {exc}")

        checked += 1

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
