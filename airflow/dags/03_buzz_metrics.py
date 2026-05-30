"""
03_buzz_metrics — Airflow DAG for "What's buzzing"-forsiden (epic #177, BUZZ-1).

Trigger-only-DAG: kaller dataportalen sitt interne endepunkt
/api/internal/buzz-compute som gjør selve aggregeringen in-process.

Begrunnelse: dataportal-poden har allerede pandas + deltalake + boto3 +
registry warm i minne. Å gjøre aggregeringen i en fersk airflow-worker krever
import av samme dependencies fra null, noe som spiser så mye minne at 256Mi
default-worker OOM-killer. Vi unngår OOM ved å trigge dataportalen i stedet.

Resultat: s3://gold/buzz_metrics/latest.json + history (skrives av dataportalen).
Kjører hver time så forsiden ikke trenger live-aggregering ved page-load.
"""

import json
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator


_PORTAL_URL = "http://dataportal.slettix-analytics.svc.cluster.local:8090"

default_args = {
    "owner":             "platform",
    "retries":           2,
    "retry_delay":       timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=3),
}


def trigger_buzz_compute(**context):
    """Kall dataportal-endepunktet og rapporter resultatet til Airflow-loggen."""
    import requests
    log = context["task_instance"].log

    api_key = os.environ.get("PORTAL_API_KEY", "dev-key-change-me")
    resp = requests.post(
        f"{_PORTAL_URL}/api/internal/buzz-compute",
        headers={"X-API-Key": api_key},
        timeout=180,
    )
    resp.raise_for_status()
    payload = resp.json()
    log.info(json.dumps({
        "event":         "buzz_compute_done",
        "products":      payload.get("products"),
        "trending":      payload.get("trending"),
        "incidents":     payload.get("incidents"),
        "questions":     payload.get("questions"),
        "elapsed_s":     payload.get("elapsed_s"),
    }))



with DAG(
    dag_id="03_buzz_metrics",
    description="BUZZ-1: pre-computer 'what's buzzing'-aggregat for dataportal-forsiden",
    schedule_interval="0 * * * *",   # hver hele time
    start_date=datetime(2026, 5, 29, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["buzz", "portal", "platform"],
    doc_md=__doc__,
) as dag:

    PythonOperator(
        task_id="trigger_buzz_compute",
        python_callable=trigger_buzz_compute,
    )
