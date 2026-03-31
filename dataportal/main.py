"""
Slettix Data Portal — FastAPI backend

Endepunkter:
  GET  /api/products                   — liste alle dataprodukter (siste versjon)
  GET  /api/products/{id}              — ett produkt med full versjonshistorikk
  POST /api/products                   — registrer / oppdater et produkt
  GET  /api/products/{id}/schema       — Delta-tabellens schema fra MinIO
  GET  /api/products/{id}/pipeline     — siste DAG-kjøring fra Airflow REST API
  GET  /api/products/{id}/quality      — siste GE-valideringsresultat fra MinIO

Swagger UI: http://localhost:8090/docs
"""

import json
import os
import sys
from datetime import datetime

import boto3
import httpx
from botocore.client import Config
from botocore.exceptions import ClientError
from deltalake import DeltaTable
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# registry.py ligger i jobs/ som monteres hit
sys.path.insert(0, "/opt/dataportal/jobs")
from registry import get, list_all, list_versions, register  # noqa: E402

# ── konfigurasjon ──────────────────────────────────────────────────────────────

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "changeme")

AIRFLOW_URL = os.environ.get("AIRFLOW_URL", "http://airflow-webserver:8080")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.environ.get("AIRFLOW_PASS", "admin")

_STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

# ── app ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Slettix Data Portal",
    version="1.0.0",
    description="REST API for dataprodukter, pipeline-status og datakvalitet.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── hjelpefunksjoner ───────────────────────────────────────────────────────────

def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )


def _resolve_product(product_id: str) -> dict:
    try:
        return get(product_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Dataprodukt '{product_id}' ikke funnet")


# ── endepunkter: produkter ─────────────────────────────────────────────────────

@app.get("/api/products", summary="Liste alle dataprodukter")
def list_products():
    """Returnerer siste versjon av hvert registrerte dataprodukt."""
    return list_all()


@app.post("/api/products", status_code=201, summary="Registrer et dataprodukt")
def register_product(manifest: dict):
    """
    Registrer et nytt produkt eller publiser en ny versjon av et eksisterende.
    Manifesten valideres mot JSON-skjemaet i conf/products/schema/.
    """
    required = {"id", "name", "domain", "owner", "version", "source_path", "format"}
    missing = required - manifest.keys()
    if missing:
        raise HTTPException(status_code=422, detail=f"Manglende felt: {sorted(missing)}")
    register(manifest)
    return {"status": "registered", "id": manifest["id"]}


@app.get("/api/products/{product_id}/schema", summary="Delta-tabellens schema")
def get_schema(product_id: str):
    """Leser schema direkte fra Delta-tabellen i MinIO via delta-rs."""
    manifest = _resolve_product(product_id)
    source = manifest["source_path"]
    try:
        dt = DeltaTable(source, storage_options=_STORAGE_OPTIONS)
        return json.loads(dt.schema().to_json())
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Kunne ikke lese schema: {exc}")


@app.get("/api/products/{product_id}/pipeline", summary="Siste pipeline-kjøring fra Airflow")
def get_pipeline_status(product_id: str):
    """
    Slår opp siste DAG-kjøring i Airflow REST API.
    Returnerer {"status": "unknown"} ved tilkoblingsfeil.
    """
    manifest = _resolve_product(product_id)
    dag_id = manifest.get("dag_id")
    if not dag_id:
        return {"status": "unknown", "reason": "Ingen dag_id i manifest"}

    try:
        resp = httpx.get(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
            params={"limit": 1, "order_by": "-execution_date"},
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            timeout=5.0,
        )
        resp.raise_for_status()
        runs = resp.json().get("dag_runs", [])
        if not runs:
            return {"status": "unknown", "reason": "Ingen kjøringer funnet"}

        run = runs[0]
        start = run.get("start_date")
        end = run.get("end_date")
        duration = None
        if start and end:
            try:
                duration_s = (
                    datetime.fromisoformat(end) - datetime.fromisoformat(start)
                ).total_seconds()
                duration = f"{duration_s:.0f}s"
            except Exception:
                pass

        return {
            "dag_id": dag_id,
            "status": run.get("state"),
            "run_id": run.get("dag_run_id"),
            "start": start,
            "end": end,
            "duration": duration,
        }
    except Exception as exc:
        return {"status": "unknown", "reason": str(exc)}


@app.get("/api/products/{product_id}/quality", summary="Siste GE-valideringsresultat")
def get_quality(product_id: str):
    """
    Leser siste Great Expectations-resultat fra
    s3://gold/quality_results/<product_id>/latest.json
    """
    _resolve_product(product_id)

    s3 = _s3_client()
    key = f"quality_results/{product_id}/latest.json"
    try:
        obj = s3.get_object(Bucket="gold", Key=key)
        return json.loads(obj["Body"].read())
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            raise HTTPException(
                status_code=404,
                detail="Ingen kvalitetsresultater funnet for dette produktet",
            )
        raise HTTPException(status_code=502, detail=f"MinIO-feil: {exc}")


@app.get("/api/products/{product_id}", summary="Hent ett dataprodukt med historikk")
def get_product(product_id: str):
    """Returnerer siste manifest og full versjonshistorikk for produktet."""
    manifest = _resolve_product(product_id)
    return {
        "manifest": manifest,
        "history": list_versions(product_id),
    }
