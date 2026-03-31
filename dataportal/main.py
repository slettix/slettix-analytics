"""
Slettix Data Portal — FastAPI backend + Jinja2 UI

API (JSON):
  GET  /api/products                   — liste alle dataprodukter
  GET  /api/products/{id}              — ett produkt med historikk
  POST /api/products                   — registrer produkt
  GET  /api/products/{id}/schema       — Delta-tabellens schema
  GET  /api/products/{id}/pipeline     — siste DAG-kjøring fra Airflow
  GET  /api/products/{id}/quality      — siste GE-valideringsresultat

UI (HTML):
  GET  /                               — produktkatalog med søk og filtrering
  GET  /products/{id}                  — detaljside per produkt
  GET  /pipelines                      — pipeline-status dashboard
"""

import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

import boto3
import httpx
from botocore.client import Config
from botocore.exceptions import ClientError
from deltalake import DeltaTable
from fastapi import FastAPI, HTTPException, Request, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.security import APIKeyHeader
from fastapi.templating import Jinja2Templates

sys.path.insert(0, "/opt/dataportal/jobs")
from registry import get, list_all, list_versions, register  # noqa: E402

# ── konfigurasjon ──────────────────────────────────────────────────────────────

MINIO_ENDPOINT  = os.environ.get("MINIO_ENDPOINT",  "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "changeme")

AIRFLOW_URL  = os.environ.get("AIRFLOW_URL",  "http://airflow-webserver:8080")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.environ.get("AIRFLOW_PASS", "admin")

PORTAL_API_KEY   = os.environ.get("PORTAL_API_KEY", "dev-key-change-me")
_api_key_scheme  = APIKeyHeader(name="X-API-Key", auto_error=False)

_STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL":           MINIO_ENDPOINT,
    "AWS_ACCESS_KEY_ID":          MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY":      MINIO_SECRET_KEY,
    "AWS_ALLOW_HTTP":             "true",
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

templates = Jinja2Templates(directory="/opt/dataportal/templates")

# ── delte hjelpefunksjoner ─────────────────────────────────────────────────────

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


def _require_access(manifest: dict, api_key: str | None) -> None:
    if manifest.get("access") == "restricted" and api_key != PORTAL_API_KEY:
        raise HTTPException(
            status_code=403,
            detail="Tilgang nektet. Oppgi gyldig API-nøkkel i X-API-Key header.",
        )


def _safe_sla(product_id: str) -> dict | None:
    try:
        obj = _s3_client().get_object(
            Bucket="gold",
            Key=f"sla_results/{product_id}/latest.json",
        )
        return json.loads(obj["Body"].read())
    except Exception:
        return None


def _compute_sla_live(manifest: dict) -> dict:
    """Beregn SLA på sparket fra Delta-tabellens historikk (fallback)."""
    product_id      = manifest["id"]
    freshness_hours = (manifest.get("quality_sla") or {}).get("freshness_hours")
    now             = datetime.now(tz=timezone.utc)

    if not freshness_hours:
        return {"product_id": product_id, "compliant": None, "reason": "Ingen SLA definert"}

    try:
        dt      = DeltaTable(manifest["source_path"], storage_options=_STORAGE_OPTIONS)
        history = dt.history(limit=1)
        if not history:
            return {"product_id": product_id, "compliant": False, "reason": "Ingen historikk"}
        ts_ms        = history[0].get("timestamp")
        last_updated = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        hours_since  = (now - last_updated).total_seconds() / 3600
        return {
            "product_id":         product_id,
            "checked_at":         now.isoformat(),
            "compliant":          hours_since <= freshness_hours,
            "last_updated":       last_updated.isoformat(),
            "hours_since_update": round(hours_since, 2),
            "freshness_hours":    freshness_hours,
        }
    except Exception as exc:
        return {"product_id": product_id, "compliant": False, "error": str(exc)}


def _safe_quality(product_id: str) -> dict | None:
    try:
        obj = _s3_client().get_object(
            Bucket="gold",
            Key=f"quality_results/{product_id}/latest.json",
        )
        return json.loads(obj["Body"].read())
    except Exception:
        return None


def _safe_pipeline(dag_id: str | None) -> dict:
    if not dag_id:
        return {"status": "unknown"}
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
            return {"status": "unknown"}
        run = runs[0]
        start = run.get("start_date")
        end   = run.get("end_date")
        duration = None
        if start and end:
            try:
                secs = (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds()
                duration = f"{secs:.0f}s"
            except Exception:
                pass
        return {
            "dag_id":   dag_id,
            "status":   run.get("state", "unknown"),
            "run_id":   run.get("dag_run_id"),
            "start":    start,
            "end":      end,
            "duration": duration,
        }
    except Exception as exc:
        return {"status": "unknown", "reason": str(exc)}


def _get_dag_timeline(dag_id: str, days: int = 7) -> list[dict]:
    """Returnerer én dict per dag siste `days` dager: {date, status}."""
    today = datetime.now(tz=timezone.utc).date()
    _priority = {"failed": 4, "running": 3, "queued": 2, "success": 1, "none": 0}
    date_status: dict[str, str] = {
        (today - timedelta(days=i)).isoformat(): "none"
        for i in range(days - 1, -1, -1)
    }
    try:
        since = (today - timedelta(days=days)).isoformat() + "T00:00:00Z"
        resp = httpx.get(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
            params={"execution_date_gte": since, "limit": 50, "order_by": "-execution_date"},
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            timeout=5.0,
        )
        resp.raise_for_status()
        for run in resp.json().get("dag_runs", []):
            exec_date = (run.get("execution_date") or "")[:10]
            state     = run.get("state", "unknown")
            if exec_date in date_status:
                if _priority.get(state, 0) > _priority.get(date_status[exec_date], 0):
                    date_status[exec_date] = state
    except Exception:
        pass
    return [{"date": d, "status": s} for d, s in date_status.items()]


def _diff_manifests(prev: dict, curr: dict) -> list[str]:
    """Returner lesbar liste over hva som endret seg fra prev til curr."""
    changes = []
    watch = ["version", "description", "owner", "source_path", "format", "access", "tags", "schema"]
    for key in watch:
        p, c = prev.get(key), curr.get(key)
        if p == c:
            continue
        if key == "schema":
            prev_cols = {f["name"] for f in (p or [])}
            curr_cols = {f["name"] for f in (c or [])}
            added   = curr_cols - prev_cols
            removed = prev_cols - curr_cols
            if added:
                changes.append(f"Schema: +{', '.join(sorted(added))}")
            if removed:
                changes.append(f"Schema: -{', '.join(sorted(removed))}")
            if not added and not removed:
                changes.append("Schema: type-endringer")
        elif key == "tags":
            changes.append(f"Tags endret")
        elif p is None:
            changes.append(f"{key} satt til '{c}'")
        else:
            changes.append(f"{key}: '{p}' → '{c}'")
    if not changes and not prev:
        changes.append("Første publisering")
    return changes


def _safe_schema(source_path: str) -> list[dict] | None:
    try:
        dt = DeltaTable(source_path, storage_options=_STORAGE_OPTIONS)
        fields = json.loads(dt.schema().to_json()).get("fields", [])
        return [{"name": f["name"], "type": str(f["type"]), "nullable": f.get("nullable", True)}
                for f in fields]
    except Exception:
        return None


# ── API: dataprodukter ─────────────────────────────────────────────────────────

@app.get("/api/products", tags=["products"], summary="Liste alle dataprodukter")
def api_list_products(api_key: str | None = Security(_api_key_scheme)):
    products = list_all()
    if api_key != PORTAL_API_KEY:
        products = [p for p in products if p.get("access") != "restricted"]
    return products


@app.post("/api/products", status_code=201, tags=["products"], summary="Registrer et dataprodukt")
def api_register_product(manifest: dict, api_key: str | None = Security(_api_key_scheme)):
    if api_key != PORTAL_API_KEY:
        raise HTTPException(status_code=403, detail="Registrering krever gyldig API-nøkkel.")
    required = {"id", "name", "domain", "owner", "version", "source_path", "format"}
    missing  = required - manifest.keys()
    if missing:
        raise HTTPException(status_code=422, detail=f"Manglende felt: {sorted(missing)}")
    register(manifest)
    return {"status": "registered", "id": manifest["id"]}


@app.get("/api/products/{product_id}/schema", tags=["products"], summary="Delta-tabellens schema")
def api_get_schema(product_id: str, api_key: str | None = Security(_api_key_scheme)):
    manifest = _resolve_product(product_id)
    _require_access(manifest, api_key)
    schema   = _safe_schema(manifest["source_path"])
    if schema is None:
        raise HTTPException(status_code=502, detail="Kunne ikke lese schema fra Delta-tabellen")
    return schema


@app.get("/api/products/{product_id}/pipeline", tags=["products"], summary="Siste pipeline-kjøring")
def api_get_pipeline(product_id: str, api_key: str | None = Security(_api_key_scheme)):
    manifest = _resolve_product(product_id)
    _require_access(manifest, api_key)
    return _safe_pipeline(manifest.get("dag_id"))


@app.get("/api/products/{product_id}/quality", tags=["products"], summary="Siste GE-valideringsresultat")
def api_get_quality(product_id: str, api_key: str | None = Security(_api_key_scheme)):
    manifest = _resolve_product(product_id)
    _require_access(manifest, api_key)
    s3  = _s3_client()
    key = f"quality_results/{product_id}/latest.json"
    try:
        obj = s3.get_object(Bucket="gold", Key=key)
        return json.loads(obj["Body"].read())
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            raise HTTPException(status_code=404, detail="Ingen kvalitetsresultater funnet")
        raise HTTPException(status_code=502, detail=str(exc))


@app.get("/api/products/{product_id}/sla", tags=["products"], summary="SLA-ferskhets-status")
def api_get_sla(product_id: str, api_key: str | None = Security(_api_key_scheme)):
    """
    Returnerer SLA-status fra siste monitor-kjøring, eller beregner det live
    fra Delta-tabellens historikk hvis ingen cached verdi finnes.
    """
    manifest = _resolve_product(product_id)
    _require_access(manifest, api_key)
    result = _safe_sla(product_id)
    if result is None:
        result = _compute_sla_live(manifest)
    return result


@app.get("/api/products/{product_id}/versions", tags=["products"], summary="Versjonshistorikk med diff")
def api_get_versions(product_id: str):
    """
    Returnerer alle publiserte versjoner av produktet, nyeste først.
    Hvert innslag inkluderer hvilke felt som endret seg siden forrige versjon.
    """
    _resolve_product(product_id)
    history = list_versions(product_id)
    if not history:
        return []

    result = []
    for i, entry in enumerate(history):
        prev_manifest = history[i + 1]["manifest"] if i + 1 < len(history) else {}
        curr_manifest = entry["manifest"]
        changes = _diff_manifests(prev_manifest, curr_manifest)
        result.append({
            "version":       entry["version"],
            "registered_at": entry["registered_at"],
            "changes":       changes,
            "manifest":      curr_manifest,
        })
    return result


@app.get("/api/products/{product_id}", tags=["products"], summary="Hent ett produkt med historikk")
def api_get_product(product_id: str):
    manifest = _resolve_product(product_id)
    return {"manifest": manifest, "history": list_versions(product_id)}


# ── UI: HTML-sider ─────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def page_catalog(request: Request):
    products = [p for p in list_all() if p.get("access") != "restricted"]
    for p in products:
        p["_quality"]  = _safe_quality(p["id"])
        p["_pipeline"] = _safe_pipeline(p.get("dag_id"))
        p["_sla"]      = _safe_sla(p["id"])
    domains  = sorted({p["domain"] for p in products})
    all_tags = sorted({tag for p in products for tag in p.get("tags", [])})
    return templates.TemplateResponse("catalog.html", {
        "request":  request,
        "products": products,
        "domains":  domains,
        "all_tags": all_tags,
    })


@app.get("/products/{product_id}", response_class=HTMLResponse, include_in_schema=False)
def page_product(request: Request, product_id: str):
    try:
        manifest = get(product_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Produkt '{product_id}' ikke funnet")
    raw_history = list_versions(product_id)
    versioned_history = []
    for i, entry in enumerate(raw_history):
        prev = raw_history[i + 1]["manifest"] if i + 1 < len(raw_history) else {}
        versioned_history.append({
            **entry,
            "changes": _diff_manifests(prev, entry["manifest"]),
        })
    schema   = _safe_schema(manifest["source_path"])
    quality  = _safe_quality(product_id)
    pipeline = _safe_pipeline(manifest.get("dag_id"))
    sla      = _safe_sla(product_id) or _compute_sla_live(manifest)
    return templates.TemplateResponse("product.html", {
        "request":  request,
        "manifest": manifest,
        "history":  versioned_history,
        "schema":   schema,
        "quality":  quality,
        "pipeline": pipeline,
        "sla":      sla,
        "airflow_url": AIRFLOW_URL.replace("airflow-webserver", "localhost").replace(":8080", ":8081"),
    })


@app.get("/pipelines", response_class=HTMLResponse, include_in_schema=False)
def page_pipelines(request: Request):
    products    = list_all()
    dag_products: dict[str, list[str]] = {}
    for p in products:
        dag_id = p.get("dag_id")
        if dag_id:
            dag_products.setdefault(dag_id, []).append(p["name"])

    dags = []
    for dag_id, product_names in dag_products.items():
        last_run  = _safe_pipeline(dag_id)
        timeline  = _get_dag_timeline(dag_id)
        dags.append({
            "dag_id":        dag_id,
            "products":      product_names,
            "last_run":      last_run,
            "timeline":      timeline,
        })

    airflow_ui = AIRFLOW_URL.replace("airflow-webserver", "localhost").replace(":8080", ":8081")
    return templates.TemplateResponse("pipelines.html", {
        "request":    request,
        "dags":       dags,
        "airflow_ui": airflow_ui,
    })
