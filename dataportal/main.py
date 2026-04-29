"""
Slettix Data Portal — FastAPI backend + Jinja2 UI

API (JSON):
  GET  /api/products                   — liste alle dataprodukter
  GET  /api/products/{id}              — ett produkt med historikk
  POST /api/products                   — registrer produkt (krever admin JWT)
  GET  /api/products/{id}/schema       — Delta-tabellens schema
  GET  /api/products/{id}/pipeline     — siste DAG-kjøring fra Airflow
  GET  /api/products/{id}/quality      — siste GE-valideringsresultat
  GET  /api/products/{id}/sla          — SLA-ferskhets-status
  GET  /api/products/{id}/sla/history  — SLA-historikk (siste 30 dager) og MTTR
  GET  /api/products/{id}/versions     — versjonshistorikk med diff
  GET  /api/products/{id}/lineage      — upstream/downstream lineage-graf
  GET  /api/browser                    — naviger MinIO og detekter Delta-tabeller
  GET  /api/products/{id}/subscribers  — list abonnenter (eier/admin)
  PATCH /api/products/{id}/subscribe   — abonner på endringer
  DELETE /api/products/{id}/subscribe  — avslutt abonnement

Auth API:
  POST /auth/login                     — logg inn, sett access/refresh cookies
  POST /auth/logout                    — slett cookies
  POST /auth/refresh                   — bytt ut refresh token, få nytt access token
  POST /auth/register                  — selvregistrering

UI (HTML):
  GET  /                               — produktkatalog
  GET  /products/{id}                  — detaljside per produkt
  GET  /pipelines                      — pipeline-status dashboard
  GET  /browse                         — Delta Lake-filutforsker
  GET  /publish                        — publiseringsformular
  POST /publish                        — publiser dataprodukt
  GET  /login                          — innloggingsside
  GET  /register                       — registreringsside
  GET  /admin                          — brukeradministrasjon (kun admin)
  POST /admin/access-requests/{id}     — godkjenn/avslå forespørsel
  POST /access-requests                — be om tilgang til restricted produkt
  GET  /domain/{domain}/contracts      — kontrakts-dashboard per domene
  GET  /domain/{domain}/contracts.csv  — eksporter kontrakt-status
  GET  /pipeline-builder               — selvbetjent pipeline-malbibliotek
  GET  /pipelines/{dag_id}/edit        — rediger pipeline-konfigurasjon

  POST /api/pipelines/preview          — forhåndsvis generert DAG-kode
  POST /api/pipelines                  — opprett og deploy ny pipeline
  PUT  /api/pipelines/{dag_id}         — oppdater og redeploy pipeline
  GET  /api/pipelines/{dag_id}/config  — hent pipeline-konfigurasjon og historikk
  GET  /api/pipelines/{dag_id}/status  — hent Airflow-status for DAG
  GET  /api/governance/pii-map          — GDPR-datakart over alle PII-kolonner
  POST /api/admin/users/{id}/domains    — Legg til domeneprivilegium (admin)
  DELETE /api/admin/users/{id}/domains/{domain} — Fjern domeneprivilegium (admin)
  GET  /governance                      — GDPR-styrings-dashboard
  GET  /api/products/{id}/quality/history — historisk kvalitetstidsserie
  GET  /api/products/{id}/anomalies       — siste anomalideteksjon
  GET  /api/products/{id}/incidents       — alle incidents for produktet
  POST /api/products/{id}/incidents       — opprett incident (system/admin)
  PATCH /api/incidents/{id}               — oppdater incident-status
  GET  /observability                     — plattformdekkende observerbarhetsdashboard
  GET  /api/search                        — semantisk full-tekst søk
  GET  /api/products/{id}/related         — relaterte produkter
  POST /api/products/{id}/generate-description — AI-generert beskrivelse
  POST /api/nl2sql                        — naturlig språk til SQL
"""

import contextlib
import contextvars
import functools
import json
import logging
import os
import pathlib
import re
import sys
import time
import urllib.parse
import uuid
from datetime import date, datetime, timedelta, timezone

import boto3
import httpx
from botocore.client import Config
from botocore.exceptions import ClientError
from deltalake import DeltaTable
from fastapi import FastAPI, Form, HTTPException, Request, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.security import APIKeyHeader
from fastapi.templating import Jinja2Templates

sys.path.insert(0, "/opt/dataportal/jobs")
from registry import (  # noqa: E402
    list_all as _registry_list_all,
    list_versions as _registry_list_versions,
    register as _registry_register,
)

import auth  # noqa: E402

# ── oppstart ───────────────────────────────────────────────────────────────────

auth.init_db()

# ── konfigurasjon ──────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",  "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "changeme")

AIRFLOW_URL  = os.environ.get("AIRFLOW_URL",  "http://airflow-webserver.slettix-analytics.svc.cluster.local:8080")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.environ.get("AIRFLOW_PASS", "admin")

PORTAL_API_KEY  = os.environ.get("PORTAL_API_KEY", "dev-key-change-me")
JUPYTER_URL     = os.environ.get("JUPYTER_URL",    "http://jupyter.slettix-analytics.svc.cluster.local:8888")
SUPERSET_URL    = os.environ.get("SUPERSET_URL",   "http://superset.slettix-analytics.svc.cluster.local:8088")

# Eksterne URL-er — åpnes i nettleseren via port-forward (eller Ingress i prod)
AIRFLOW_EXTERNAL_URL  = os.environ.get("AIRFLOW_EXTERNAL_URL",  "http://localhost:8080")
JUPYTER_EXTERNAL_URL  = os.environ.get("JUPYTER_EXTERNAL_URL",  "http://localhost:8888")
SUPERSET_EXTERNAL_URL = os.environ.get("SUPERSET_EXTERNAL_URL", "http://localhost:8088")
SUPERSET_DB_ID        = int(os.environ.get("SUPERSET_DB_ID", "1"))
MINIO_EXTERNAL_URL    = os.environ.get("MINIO_EXTERNAL_URL",    "http://localhost:9001")
NOTEBOOKS_DIR   = os.environ.get("NOTEBOOKS_DIR",  "/opt/dataportal/notebooks")
DAGS_DIR        = os.environ.get("DAGS_DIR",       "/opt/airflow/dags")
_api_key_scheme = APIKeyHeader(name="X-API-Key", auto_error=False)

_STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL":           MINIO_ENDPOINT,
    "AWS_ACCESS_KEY_ID":          MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY":      MINIO_SECRET_KEY,
    "AWS_ALLOW_HTTP":             "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

# ── observability: request-timing ──────────────────────────────────────────────

SLOW_REQUEST_MS = int(os.environ.get("SLOW_REQUEST_MS", "1000"))

_timing_logger = logging.getLogger("dataportal.timing")
if not _timing_logger.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter("%(message)s"))
    _timing_logger.addHandler(_h)
    _timing_logger.setLevel(logging.INFO)
    _timing_logger.propagate = False

_stage_timings: contextvars.ContextVar[list[dict] | None] = contextvars.ContextVar(
    "_stage_timings", default=None
)


@contextlib.contextmanager
def timer(name: str):
    """Mål forløpt tid for en stage og legg den til pågående requests timing-liste.

    Bruk:
        with timer("s3.quality.latest"):
            data = s3.get_object(...)
    Stages logges samlet av `timing_middleware` ved request-slutt. Brukt utenfor
    en request logger den én linje umiddelbart.
    """
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
        stages = _stage_timings.get()
        if stages is not None:
            stages.append({"name": name, "ms": elapsed_ms})
        else:
            _timing_logger.info(json.dumps(
                {"event": "stage", "name": name, "ms": elapsed_ms},
                ensure_ascii=False,
            ))


def timed(name: str):
    """Dekoratør som wrapper en synkron funksjon med `timer(name)`."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            with timer(name):
                return fn(*args, **kwargs)
        return wrapper
    return decorator


# ── registry-cache (PERF-2) ────────────────────────────────────────────────────
#
# Lagdelt cache rundt `registry.list_all()` og `registry.list_versions(pid)`:
#   1. Request-scope dedup (samme request → samme liste, via ContextVar)
#   2. Cross-request TTL-cache (default 60s)
#   3. Fallback til Delta Lake-read
# Cache-treff/miss logges på DEBUG-nivå via `dataportal.timing`-loggeren.
# Kall til `register()` invaliderer cachene.

_REGISTRY_CACHE_TTL_S = float(os.environ.get("REGISTRY_CACHE_TTL_S", "60"))

_list_all_ttl_cache: tuple[list[dict], float] | None = None
_list_versions_ttl_cache: dict[str, tuple[list[dict], float]] = {}

_request_list_all_cv: contextvars.ContextVar[list[dict] | None] = contextvars.ContextVar(
    "_request_list_all_cv", default=None
)


def _cache_log(name: str, result: str) -> None:
    if _timing_logger.isEnabledFor(logging.DEBUG):
        _timing_logger.debug(json.dumps(
            {"event": "cache", "name": name, "result": result},
            ensure_ascii=False,
        ))


def list_all() -> list[dict]:
    """Cachet wrapper rundt `registry.list_all()`."""
    global _list_all_ttl_cache
    cached_req = _request_list_all_cv.get()
    if cached_req is not None:
        _cache_log("registry.list_all", "request_hit")
        return cached_req

    now = time.monotonic()
    ttl = _list_all_ttl_cache
    if ttl is not None:
        value, expires_at = ttl
        if now < expires_at:
            _cache_log("registry.list_all", "ttl_hit")
            _request_list_all_cv.set(value)
            return value

    _cache_log("registry.list_all", "miss")
    value = _registry_list_all()
    _list_all_ttl_cache = (value, now + _REGISTRY_CACHE_TTL_S)
    _request_list_all_cv.set(value)
    return value


def list_versions(product_id: str) -> list[dict]:
    """Cachet wrapper rundt `registry.list_versions(product_id)`."""
    now = time.monotonic()
    cached = _list_versions_ttl_cache.get(product_id)
    if cached is not None:
        value, expires_at = cached
        if now < expires_at:
            _cache_log("registry.list_versions", "ttl_hit")
            return value

    _cache_log("registry.list_versions", "miss")
    value = _registry_list_versions(product_id)
    _list_versions_ttl_cache[product_id] = (value, now + _REGISTRY_CACHE_TTL_S)
    return value


def get(product_id: str) -> dict:
    """Latest manifest for `product_id` — leser fra cachet `list_all()`."""
    products = {p["id"]: p for p in list_all()}
    if product_id not in products:
        raise KeyError(f"Data product '{product_id}' not found in registry")
    return products[product_id]


def register(manifest: dict) -> None:
    """Skriv til Delta og invalider cacher (write-through)."""
    _registry_register(manifest)
    _invalidate_registry_cache(manifest.get("id"))


def _invalidate_registry_cache(product_id: str | None = None) -> None:
    """Tøm TTL- og request-scope-cache. Sikrer at neste les ser nye data."""
    global _list_all_ttl_cache
    _list_all_ttl_cache = None
    if product_id:
        _list_versions_ttl_cache.pop(product_id, None)
    else:
        _list_versions_ttl_cache.clear()
    _request_list_all_cv.set(None)


# ── app ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Slettix Data Portal",
    version="2.0.0",
    description="REST API for dataprodukter, pipeline-status og datakvalitet.",
)

@app.middleware("http")
async def track_product_views(request, call_next):
    """Logg produktvisninger anonymt til bruksstatistikk."""
    response = await call_next(request)
    path = request.url.path
    # Matche /products/{id} og /api/products/{id} (GET, ikke sub-paths)
    import re as _re
    m = _re.match(r"^/(?:api/)?products/([^/]+)$", path)
    if m and request.method == "GET" and response.status_code < 400:
        pid  = m.group(1)
        user = auth.get_current_user(request)
        uid  = user["id"] if user else None
        auth.track_usage(pid, "view", uid)
    return response


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def timing_middleware(request, call_next):
    """Logg method/path/status/duration_ms + per-stage timings per request.

    Registrert sist → outermost middleware → måler total tid inkludert øvrige
    middlewares. Slow-request-terskel styres av `SLOW_REQUEST_MS` env-var.
    """
    token       = _stage_timings.set([])
    req_token   = _request_list_all_cv.set(None)
    request_id  = uuid.uuid4().hex[:12]
    start       = time.perf_counter()
    status_code = 500
    try:
        response    = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        stages      = _stage_timings.get() or []
        _stage_timings.reset(token)
        _request_list_all_cv.reset(req_token)
        record = {
            "event":       "request",
            "request_id":  request_id,
            "method":      request.method,
            "path":        request.url.path,
            "status":      status_code,
            "duration_ms": duration_ms,
        }
        if stages:
            record["stages"] = stages
        msg = json.dumps(record, ensure_ascii=False)
        if duration_ms >= SLOW_REQUEST_MS:
            _timing_logger.warning(msg)
        else:
            _timing_logger.info(msg)


def _prefill_versions_cache() -> int:
    """Les hele registry-tabellen ÉN gang og populer per-produkt versions-cache.

    Erstatter sekvensiell/parallell `list_versions(pid)` × N (som ville lest
    hele tabellen N ganger). Returnerer antall produkter som ble cachet.
    """
    import json as _json
    from deltalake import DeltaTable

    try:
        sys.path.insert(0, "/opt/dataportal/jobs")
        from registry import REGISTRY_PATH, _STORAGE_OPTIONS as _REG_STORAGE  # noqa: E402

        dt = DeltaTable(REGISTRY_PATH, storage_options=_REG_STORAGE)
        df = dt.to_pandas().sort_values("registered_at", ascending=False)
    except Exception:
        return 0

    now = time.monotonic()
    expires_at = now + _REGISTRY_CACHE_TTL_S

    # Grupper per product_id og bygg samme struktur som registry.list_versions
    by_pid: dict[str, list[dict]] = {}
    for _, row in df.iterrows():
        pid = row["product_id"]
        by_pid.setdefault(pid, []).append({
            "version":       row["version"],
            "registered_at": row["registered_at"],
            "manifest":      _json.loads(row["manifest_json"]),
        })

    for pid, versions in by_pid.items():
        _list_versions_ttl_cache[pid] = (versions, expires_at)
    return len(by_pid)


async def _warmup_task() -> None:
    """PERF-9: prefetche tunge ressurser så første sidevisning er rask.

    Kjøres som detached background-task fra `_kickoff_warmup()`. Første runde
    logger hvert stage; deretter loop'er den med `_REGISTRY_CACHE_TTL_S * 0.7`s
    intervall for å refreshe cachen *før* TTL utløper (proactive refresh).
    Stille på succession; logger kun feil etter første runde.
    """
    import asyncio

    refresh_interval_s = max(_REGISTRY_CACHE_TTL_S * 0.7, 10)
    first_run = True

    async def _stage(name: str, fn, *args):
        start = time.perf_counter()
        try:
            result = await asyncio.to_thread(fn, *args)
            ms = round((time.perf_counter() - start) * 1000, 2)
            if first_run:
                # Logg kun små numeriske resultater (f.eks. produkt-antall fra prefill)
                extra = {"count": result} if isinstance(result, int) else {}
                _timing_logger.info(json.dumps(
                    {"event": "warmup", "name": name, "ms": ms, "ok": True, **extra},
                    ensure_ascii=False,
                ))
        except Exception as exc:
            ms = round((time.perf_counter() - start) * 1000, 2)
            _timing_logger.info(json.dumps(
                {"event": "warmup", "name": name, "ms": ms, "ok": False, "error": str(exc)[:200]},
                ensure_ascii=False,
            ))

    while True:
        # 1) Re-les list_all (invaliderer TTL og setter ny verdi)
        _invalidate_registry_cache()
        await _stage("registry.list_all", list_all)
        # 2) Prefill alle list_versions-entries i én tabell-read
        await _stage("registry.list_versions.prefill", _prefill_versions_cache)

        if first_run:
            # Ping Airflow webserver så connection pool er warm
            await _stage(
                "airflow.health",
                lambda: _HTTPX_CLIENT.get(f"{AIRFLOW_URL}/health", timeout=3.0),
            )
            _timing_logger.info(json.dumps(
                {"event": "warmup", "name": "complete", "ok": True, "refresh_interval_s": refresh_interval_s},
                ensure_ascii=False,
            ))
            first_run = False

        await asyncio.sleep(refresh_interval_s)


@app.on_event("startup")
async def _kickoff_warmup() -> None:
    """Spinner opp warmup-tasken og returnerer umiddelbart, slik at readiness-
    proben på `/health` ikke blokkeres av warmup-arbeidet."""
    import asyncio
    asyncio.create_task(_warmup_task())


templates = Jinja2Templates(directory="/opt/dataportal/templates")

# ── hjelpefunksjoner ───────────────────────────────────────────────────────────

# PERF-1: gjenbruk klient-instanser på tvers av requests (eliminerer
# cold-connection-overhead, signature-config og DNS-oppslag per kall).

_S3_SINGLETON = None


def _s3_client():
    global _S3_SINGLETON
    if _S3_SINGLETON is None:
        _S3_SINGLETON = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
        )
    return _S3_SINGLETON


# Modul-nivå httpx.Client med connection pool. K8s API-kallet bruker egen
# klient pga ulik SSL-trust (custom CA fra service account-mount).
_HTTPX_CLIENT = httpx.Client(
    timeout=httpx.Timeout(10.0),
    limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
)


def _resolve_product(product_id: str) -> dict:
    try:
        return get(product_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Dataprodukt '{product_id}' ikke funnet")


def _check_product_access(manifest: dict, user: dict | None, api_key: str | None) -> bool:
    """Returnerer True hvis brukeren/API-nøkkelen har tilgang til produktet."""
    if manifest.get("access") != "restricted":
        return True
    if api_key == PORTAL_API_KEY:
        return True
    if user and user["role"] == "admin":
        return True
    if user and auth.user_has_product_access(user["id"], manifest["id"]):
        return True
    return False


def _require_access(manifest: dict, user: dict | None, api_key: str | None) -> None:
    if not _check_product_access(manifest, user, api_key):
        raise HTTPException(
            status_code=403,
            detail="Tilgang nektet. Logg inn og be om tilgang, eller bruk gyldig API-nøkkel.",
        )


def _require_admin_api(user: dict | None) -> None:
    if not user or user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Krever admin-rolle.")


@timed("s3.sla.latest")
def _safe_sla(product_id: str) -> dict | None:
    try:
        obj = _s3_client().get_object(
            Bucket="gold",
            Key=f"sla_results/{product_id}/latest.json",
        )
        return json.loads(obj["Body"].read())
    except Exception:
        return None


@timed("s3.sla.history")
def _safe_sla_history(product_id: str, limit: int = 60) -> list[dict]:
    """Les tidsserie av SLA-resultater fra MinIO history/-mappe."""
    try:
        s3   = _s3_client()
        resp = s3.list_objects_v2(
            Bucket="gold",
            Prefix=f"sla_results/{product_id}/history/",
        )
        keys = sorted(
            [o["Key"] for o in resp.get("Contents", [])],
            reverse=True,
        )[:limit]
        result = []
        for key in keys:
            try:
                obj = s3.get_object(Bucket="gold", Key=key)
                result.append(json.loads(obj["Body"].read()))
            except Exception:
                pass
        return list(reversed(result))  # kronologisk rekkefølge
    except Exception:
        return []


@timed("compute.sla_compliance_pct")
def _sla_compliance_pct(
    product_id: str,
    days: int = 30,
    manifest: dict | None = None,
    max_age_hours: float = 24.0,
) -> float | None:
    """Beregn SLA-overholdelse i prosent siste N dager.

    Bruker precomputed `sla_compliance_30d` fra manifest når den er tilgjengelig
    og fersk (< `max_age_hours` gammel — default 24t). Faller tilbake til
    live-beregning fra MinIO-historikk (1440 sekvensielle reads i verste fall).

    PERF-4: precompute skjer i `airflow/dags/02_sla_monitor.py`.
    """
    if manifest is None:
        try:
            manifest = get(product_id)
        except KeyError:
            manifest = {}
    cached_pct = manifest.get("sla_compliance_30d")
    cached_at  = manifest.get("sla_aggregate_computed_at")
    if cached_pct is not None and cached_at:
        try:
            computed_at = datetime.fromisoformat(cached_at)
            if computed_at.tzinfo is None:
                computed_at = computed_at.replace(tzinfo=timezone.utc)
            age_h = (datetime.now(tz=timezone.utc) - computed_at).total_seconds() / 3600
            if age_h < max_age_hours:
                return float(cached_pct)
        except (ValueError, TypeError):
            pass

    history = _safe_sla_history(product_id, limit=days * 48)  # maks 48 sjekker/dag
    if not history:
        return None
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)
    relevant = []
    for entry in history:
        try:
            checked_at = datetime.fromisoformat(entry["checked_at"])
            if checked_at >= cutoff:
                relevant.append(entry)
        except Exception:
            pass
    if not relevant:
        return None
    compliant_count = sum(1 for e in relevant if e.get("compliant") is True)
    return round(compliant_count / len(relevant) * 100, 1)


@timed("db.mttr")
def _mttr(product_id: str) -> float | None:
    """Beregn Mean Time To Resolve (timer) for løste incidents på produktet."""
    incidents = auth.list_incidents(product_id=product_id)
    resolved = [
        i for i in incidents
        if i.get("status") == "resolved"
        and i.get("created_at")
        and i.get("resolved_at")
    ]
    if not resolved:
        return None
    total_hours = 0.0
    count = 0
    for inc in resolved:
        try:
            created  = datetime.fromisoformat(inc["created_at"])
            resolved_at = datetime.fromisoformat(inc["resolved_at"])
            hours = (resolved_at - created).total_seconds() / 3600
            if hours >= 0:
                total_hours += hours
                count += 1
        except Exception:
            pass
    if count == 0:
        return None
    return round(total_hours / count, 1)


@timed("compute.sla_live")
def _compute_sla_live(manifest: dict) -> dict:
    product_id      = manifest["id"]
    freshness_hours = (manifest.get("quality_sla") or {}).get("freshness_hours")
    now             = datetime.now(tz=timezone.utc)
    if not freshness_hours:
        return {"product_id": product_id, "compliant": None, "reason": "Ingen SLA definert"}
    try:
        # Bruk cachet last_updated fra pipeline_stats hvis tilgjengelig
        cached_ts = manifest.get("last_updated")
        if cached_ts:
            last_updated = datetime.fromisoformat(cached_ts)
            if last_updated.tzinfo is None:
                last_updated = last_updated.replace(tzinfo=timezone.utc)
        else:
            dt      = DeltaTable(manifest["source_path"], storage_options=_STORAGE_OPTIONS)
            history = dt.history(limit=1)
            if not history:
                return {"product_id": product_id, "compliant": False, "reason": "Ingen historikk"}
            ts_ms        = history[0].get("timestamp")
            last_updated = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        hours_since = (now - last_updated).total_seconds() / 3600
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


@timed("s3.quality.latest")
def _safe_quality(product_id: str) -> dict | None:
    try:
        obj = _s3_client().get_object(
            Bucket="gold",
            Key=f"quality_results/{product_id}/latest.json",
        )
        return json.loads(obj["Body"].read())
    except Exception:
        return None


@timed("s3.quality.history")
def _safe_quality_history(product_id: str, limit: int = 30) -> list[dict]:
    """Les tidsserie av kvalitetsresultater fra MinIO history/-mappe."""
    try:
        s3   = _s3_client()
        resp = s3.list_objects_v2(
            Bucket="gold",
            Prefix=f"quality_results/{product_id}/history/",
        )
        keys = sorted(
            [o["Key"] for o in resp.get("Contents", [])],
            reverse=True,
        )[:limit]
        result = []
        for key in keys:
            try:
                obj = s3.get_object(Bucket="gold", Key=key)
                result.append(json.loads(obj["Body"].read()))
            except Exception:
                pass
        return list(reversed(result))  # kronologisk rekkefølge
    except Exception:
        return []


@timed("s3.quality.anomalies")
def _safe_anomalies(product_id: str) -> dict | None:
    """Les siste anomaliresultat fra MinIO."""
    try:
        obj = _s3_client().get_object(
            Bucket="gold",
            Key=f"quality_results/{product_id}/anomalies/latest.json",
        )
        return json.loads(obj["Body"].read())
    except Exception:
        return None


def _build_search_index(products: list[dict]) -> "sqlite3.Connection":
    """
    Bygg en in-memory SQLite FTS5-indeks over alle produkter.
    Indekserer: navn, beskrivelse, domene, eier, tags og kolonnenavn.
    Returnerer åpen tilkobling (lukkes av kalleren).
    """
    import sqlite3 as _sqlite3
    conn = _sqlite3.connect(":memory:")
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS fts USING fts5(
            product_id UNINDEXED,
            content,
            tokenize='unicode61'
        )
    """)
    for m in products:
        cols   = " ".join(c["name"] for c in (m.get("schema") or []) if c.get("name"))
        tags   = " ".join(m.get("tags") or [])
        text   = " ".join(filter(None, [
            m.get("name", ""),
            m.get("description", ""),
            m.get("domain", ""),
            m.get("owner", ""),
            tags,
            cols,
        ]))
        conn.execute("INSERT INTO fts (product_id, content) VALUES (?,?)", (m["id"], text))
    conn.commit()
    return conn


def _search_products(query: str, products: list[dict], limit: int = 20) -> list[dict]:
    """
    Søk i produktkatalogen med FTS5. Returnerer produkter med match-info.
    Faller tilbake til enkel substring-match hvis FTS feiler.
    """
    import sqlite3 as _sqlite3
    idx = {m["id"]: m for m in products}

    # Legg til bruksstatistikk i rangeringen
    usage  = {r["product_id"]: r["views"] for r in auth.get_usage_counts(30)}

    try:
        conn = _build_search_index(products)
        # Escape FTS5 special chars
        safe_q = query.replace('"', '""')
        rows   = conn.execute(
            'SELECT product_id, snippet(fts, 1, "<mark>", "</mark>", "…", 20) AS snip '
            'FROM fts WHERE fts MATCH ? ORDER BY rank LIMIT ?',
            (safe_q, limit),
        ).fetchall()
        conn.close()
        results = []
        for row in rows:
            pid = row[0]
            m   = idx.get(pid)
            if not m:
                continue
            # Finn kolonnetreff
            col_matches = [
                c["name"] for c in (m.get("schema") or [])
                if query.lower() in (c.get("name") or "").lower()
            ]
            results.append({
                **m,
                "snippet":     row[1],
                "col_matches": col_matches,
                "views":       usage.get(pid, 0),
            })
        return results
    except Exception:
        # Fallback: enkel substring-match
        q = query.lower()
        results = []
        for m in products:
            text = " ".join(filter(None, [
                m.get("name",""), m.get("description",""),
                m.get("domain",""), " ".join(m.get("tags") or []),
            ])).lower()
            if q in text:
                col_matches = [c["name"] for c in (m.get("schema") or []) if q in c.get("name","").lower()]
                results.append({**m, "snippet": "", "col_matches": col_matches, "views": usage.get(m["id"], 0)})
        return results[:limit]


@timed("compute.related")
def _related_products(product_id: str, manifest: dict, all_products: list[dict], limit: int = 4) -> list[dict]:
    """
    Finn relaterte produkter basert på:
    1. Samme domene (+3 poeng)
    2. Overlappende kolonnenavn (+2 per overlapp, maks 6)
    3. Del av samme lineage-tre (+2)
    4. Samme eier (+1)
    """
    domain    = manifest.get("domain", "")
    owner     = manifest.get("owner", "")
    my_cols   = {c["name"] for c in (manifest.get("schema") or [])}
    my_lineage = set(manifest.get("source_products") or [])

    # Alle produkter i lineage-treet (upstream + downstream)
    downstream = {m["id"] for m in all_products if product_id in (m.get("source_products") or [])}
    lineage_ids = my_lineage | downstream

    scored = []
    for m in all_products:
        if m["id"] == product_id:
            continue
        score = 0
        if m.get("domain") == domain:
            score += 3
        other_cols = {c["name"] for c in (m.get("schema") or [])}
        overlap = len(my_cols & other_cols)
        score += min(overlap * 2, 6)
        if m["id"] in lineage_ids:
            score += 2
        if m.get("owner") == owner:
            score += 1
        if score > 0:
            scored.append((score, m))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [m for _, m in scored[:limit]]


_K8S_API_BASE   = "https://kubernetes.default.svc"
_K8S_TOKEN_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_K8S_CA_FILE    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"


def _k8s_get(path: str) -> dict:
    """Gjør et GET-kall mot in-cluster Kubernetes API."""
    try:
        token = pathlib.Path(_K8S_TOKEN_FILE).read_text().strip()
    except FileNotFoundError:
        raise RuntimeError("Ikke in-cluster — ingen service account token")
    resp = httpx.get(
        f"{_K8S_API_BASE}{path}",
        headers={"Authorization": f"Bearer {token}"},
        verify=_K8S_CA_FILE,
        timeout=5.0,
    )
    resp.raise_for_status()
    return resp.json()


@timed("k8s.idp_status")
def _safe_idp_status(manifest: dict) -> dict | None:
    """Henter SparkApplication-status for streaming IDP knyttet til dette produktet."""
    product_id = manifest.get("id", "")
    if not product_id:
        return None
    try:
        result = _k8s_get(
            "/apis/sparkoperator.k8s.io/v1beta2/namespaces/slettix-analytics/sparkapplications"
        )
        apps = [
            item for item in result.get("items", [])
            if item.get("metadata", {}).get("annotations", {}).get("slettix.io/product-id") == product_id
        ]
        if not apps:
            return None
        app = apps[0]
        state     = app.get("status", {}).get("applicationState", {}).get("state", "UNKNOWN")
        submitted = app.get("status", {}).get("lastSubmissionAttemptTime")
        topics    = app.get("metadata", {}).get("annotations", {}).get("slettix.io/kafka-topics", "")
        app_name  = app.get("metadata", {}).get("name", "")
        uptime    = None
        if submitted and state == "RUNNING":
            try:
                dt    = datetime.fromisoformat(submitted.replace("Z", "+00:00"))
                delta = datetime.now(timezone.utc) - dt
                hours = int(delta.total_seconds() // 3600)
                mins  = int((delta.total_seconds() % 3600) // 60)
                uptime = f"{hours}t {mins}m" if hours else f"{mins}m"
            except Exception:
                pass
        return {
            "state":     state,
            "app_name":  app_name,
            "submitted": submitted,
            "uptime":    uptime,
            "topics":    [t.strip() for t in topics.split(",") if t.strip()],
        }
    except Exception:
        return None


@timed("airflow.pipeline")
def _safe_pipeline(dag_id: str | None) -> dict:
    if not dag_id:
        return {"status": "unknown"}
    try:
        resp = _HTTPX_CLIENT.get(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
            params={"limit": 1, "order_by": "-execution_date"},
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            timeout=5.0,
        )
        resp.raise_for_status()
        runs = resp.json().get("dag_runs", [])
        if not runs:
            return {"status": "unknown"}
        run      = runs[0]
        start    = run.get("start_date")
        end      = run.get("end_date")
        duration = None
        if start and end:
            try:
                secs     = (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds()
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


@timed("airflow.dag_timeline")
def _get_dag_timeline(dag_id: str, days: int = 7) -> list[dict]:
    today = datetime.now(tz=timezone.utc).date()
    _priority = {"failed": 4, "running": 3, "queued": 2, "success": 1, "none": 0}
    date_status: dict[str, str] = {
        (today - timedelta(days=i)).isoformat(): "none"
        for i in range(days - 1, -1, -1)
    }
    try:
        since = (today - timedelta(days=days)).isoformat() + "T00:00:00Z"
        resp  = _HTTPX_CLIENT.get(
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


# ── Lineage-hjelpere (#65) ─────────────────────────────────────────────────

_MEDALLION_ORDER = ["raw", "bronze", "silver", "gold", "analytics"]


def _source_layer(source_path: str) -> str:
    """Utled medallion-lag fra source_path (f.eks. s3://silver/hr/employees → 'silver')."""
    path  = (source_path or "").replace("s3a://", "").replace("s3://", "")
    layer = path.split("/")[0].lower()
    return layer if layer in _MEDALLION_ORDER else "gold"


def _build_upstream(product_id: str, idx: dict, visited: set, depth: int = 0) -> list[dict]:
    """Bygg rekursivt upstream-tre. Bruker visited-set mot sirkulære avhengigheter."""
    if depth >= 5:
        return []
    nodes = []
    manifest = idx.get(product_id, {})
    for sid in manifest.get("source_products") or []:
        if sid in visited:
            continue
        src = idx.get(sid)
        nodes.append({
            "id":           sid,
            "name":         src.get("name", sid) if src else sid,
            "product_type": src.get("product_type", "source") if src else "unknown",
            "layer":        _source_layer(src.get("source_path", "")) if src else "unknown",
            "upstream":     _build_upstream(sid, idx, visited | {sid}, depth + 1),
        })
    return nodes


def _find_downstream(product_id: str, idx: dict) -> list[dict]:
    """Finn alle produkter som refererer til product_id i source_products."""
    result = []
    for pid, m in idx.items():
        if product_id in (m.get("source_products") or []):
            result.append({
                "id":           pid,
                "name":         m.get("name", pid),
                "product_type": m.get("product_type", "source"),
            })
    return result


def _has_pii_access(user: dict | None) -> bool:
    """Admin og pii_access-rolle ser PII-kolonner i klartekst."""
    return auth.has_pii_access(user)


def _mask_schema(schema: list[dict], pii_ok: bool) -> list[dict]:
    """Masker PII-kolonner for brukere uten PII-tilgang."""
    if pii_ok or not schema:
        return schema
    result = []
    for col in schema:
        if col.get("pii"):
            result.append({**col, "type": "***", "description": "*** (PII — begrenset tilgang)"})
        else:
            result.append(col)
    return result


def _safe_node_id(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "_", s)


def _mermaid_label(text: str) -> str:
    """Escape tekst for bruk i Mermaid-nodel."""
    return text.replace('"', "'").replace("\n", " ")


def _mermaid_lineage(manifest: dict, upstream: list, downstream: list) -> str:
    """Generer Mermaid flowchart-streng for lineage-visualisering."""
    product_id   = manifest["id"]
    product_name = _mermaid_label(manifest.get("name", product_id))
    product_type = manifest.get("product_type", "source")
    layer        = _source_layer(manifest.get("source_path", ""))
    curr_node    = _safe_node_id(product_id)
    lines        = ["flowchart LR"]

    if product_type != "analytical":
        # ── Kildeprodukter: vis medallion-sti ──────────────────────────────
        try:
            depth = _MEDALLION_ORDER.index(layer)
        except ValueError:
            depth = 3  # fallback til gold

        layers_to_show = _MEDALLION_ORDER[: depth + 1]

        lines.append('  EXT(["☁ Ekstern kilde"])')
        lines.append("  style EXT fill:#f8f9fa,stroke:#adb5bd,color:#6c757d")
        prev = "EXT"

        for l in layers_to_show:
            nid = l.upper()
            if l == layer:
                lines.append(f'  {nid}["{l}\\n{product_name}"]')
                lines.append(f"  style {nid} fill:#0d6efd,color:#fff,stroke:#0a58ca")
            else:
                lines.append(f'  {nid}["{l}"]')
                lines.append(f"  style {nid} fill:#e9ecef,color:#495057,stroke:#dee2e6")
            lines.append(f"  {prev} --> {nid}")
            prev = nid

        for d in downstream[:4]:
            did   = _safe_node_id(d["id"])
            dname = _mermaid_label(d["name"])
            lines.append(f'  {did}(["{dname}"])')
            lines.append(f"  style {did} fill:#198754,color:#fff,stroke:#157347")
            lines.append(f"  {prev} --> {did}")
            lines.append(f'  click {did} "/products/{d["id"]}"')

    else:
        # ── Analytiske produkter: upstream → current → downstream ──────────
        lines.append(f'  {curr_node}(["{product_name}"])')
        lines.append(f"  style {curr_node} fill:#198754,color:#fff,stroke:#157347")

        if upstream:
            for u in upstream:
                uid   = _safe_node_id(u["id"])
                uname = _mermaid_label(u["name"])
                if u["product_type"] == "analytical":
                    lines.append(f'  {uid}(["{uname}"])')
                    lines.append(f"  style {uid} fill:#DD8452,color:#fff,stroke:#b86c3e")
                else:
                    ulayer = u["layer"]
                    uid_href = u["id"]
                    lines.append(f'  {uid}["{uname}\\n{ulayer}"]')
                    lines.append(f"  style {uid} fill:#4C72B0,color:#fff,stroke:#3d5a8e")
                lines.append(f"  {uid} --> {curr_node}")
                uid_href = u["id"]
                lines.append(f'  click {uid} "/products/{uid_href}"')
        else:
            lines.append('  EXT(["☁ Ingen registrerte\\nkilder"])')
            lines.append("  style EXT fill:#f8f9fa,stroke:#adb5bd,color:#6c757d")
            lines.append(f"  EXT --> {curr_node}")

        for d in downstream[:4]:
            did   = _safe_node_id(d["id"])
            dname = _mermaid_label(d["name"])
            lines.append(f'  {did}(["{dname}"])')
            lines.append(f"  style {did} fill:#DD8452,color:#fff,stroke:#b86c3e")
            lines.append(f"  {curr_node} --> {did}")
            lines.append(f'  click {did} "/products/{d["id"]}"')

    return "\n".join(lines)


def _check_schema_compatibility(
    old_schema: list[dict] | None,
    new_schema: list[dict] | None,
    mode: str,
) -> list[str]:
    """
    Sjekker skjemakompatibilitet etter Confluent-semantikk.
    Returnerer liste med bruddmeldinger. Tom liste = kompatibelt.

    BACKWARD: nye nullable-felter OK, fjerning/typeendring blokkeres
    FORWARD:  fjerning OK, nye felter blokkeres
    FULL:     ingen strukturelle endringer tillatt
    NONE:     ingen sjekk
    """
    if mode == "NONE" or not old_schema or not new_schema:
        return []
    old_cols = {f["name"]: f for f in old_schema}
    new_cols = {f["name"]: f for f in new_schema}
    removed  = set(old_cols) - set(new_cols)
    added    = set(new_cols) - set(old_cols)
    violations = []
    if mode in ("BACKWARD", "FULL"):
        for col in removed:
            violations.append(f"Kolonne '{col}' er fjernet (BACKWARD-brudd)")
        for col in old_cols:
            if col in new_cols and old_cols[col].get("type") != new_cols[col].get("type"):
                violations.append(
                    f"Type for '{col}' endret fra '{old_cols[col].get('type')}'"
                    f" til '{new_cols[col].get('type')}' (BACKWARD-brudd)"
                )
    if mode in ("FORWARD", "FULL"):
        for col in added:
            violations.append(f"Ny kolonne '{col}' lagt til (FORWARD-brudd)")
    return violations


SLACK_WEBHOOK_URL  = os.environ.get("SLACK_WEBHOOK_URL", "")
ANTHROPIC_API_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")


def _notify_subscribers(product_id: str, subject: str, body: str) -> None:
    """Send Slack-varsling til produktets abonnenter (best-effort)."""
    subscribers = auth.list_subscribers(product_id)
    if not subscribers:
        return
    emails = ", ".join(s["email"] for s in subscribers)
    full_msg = f"*{subject}*\n{body}\nAbonnenter: {emails}"
    if SLACK_WEBHOOK_URL:
        try:
            _HTTPX_CLIENT.post(SLACK_WEBHOOK_URL, json={"text": full_msg}, timeout=5.0)
        except Exception:
            pass


def _diff_manifests(prev: dict, curr: dict) -> list[str]:
    changes = []
    watch   = ["version", "description", "owner", "source_path", "format", "access", "tags", "schema"]
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
            changes.append("Tags endret")
        elif p is None:
            changes.append(f"{key} satt til '{c}'")
        else:
            changes.append(f"{key}: '{p}' → '{c}'")
    if not changes and not prev:
        changes.append("Første publisering")
    return changes


@timed("delta.schema")
def _safe_schema(source_path: str) -> list[dict] | None:
    try:
        dt     = DeltaTable(source_path, storage_options=_STORAGE_OPTIONS)
        fields = json.loads(dt.schema().to_json()).get("fields", [])
        return [{"name": f["name"], "type": str(f["type"]), "nullable": f.get("nullable", True)}
                for f in fields]
    except Exception:
        return None


def _merge_schema(delta_schema: list[dict] | None, manifest_schema: list[dict] | None) -> list[dict] | None:
    """
    Slå sammen Delta-tabellens schema (type, nullable) med manifestets schema-metadata
    (pii, sensitivity, description). Manifestet er kilden til sannhet for metadata.

    Kolonner som er i manifestet men ikke ennå i Delta-tabellen vises med pending=True
    (f.eks. nye felter dokumentert i manifest før første pipeline-kjøring).
    """
    if not delta_schema and not manifest_schema:
        return None
    # Bygg oppslag fra manifest-schema (kan mangle for eldre produkter)
    meta = {col["name"]: col for col in (manifest_schema or [])}
    base = delta_schema or [{"name": c["name"], "type": c.get("type", ""), "nullable": True}
                             for c in (manifest_schema or [])]
    result = []
    seen: set[str] = set()
    for field in base:
        m = meta.get(field["name"], {})
        merged = {**field}
        if m.get("pii"):
            merged["pii"] = True
        if m.get("sensitivity"):
            merged["sensitivity"] = m["sensitivity"]
        if m.get("description"):
            merged["description"] = m["description"]
        result.append(merged)
        seen.add(field["name"])
    # Legg til kolonner dokumentert i manifest men ikke ennå i Delta-tabellen
    for col in (manifest_schema or []):
        if col["name"] not in seen:
            merged = {
                "name":     col["name"],
                "type":     col.get("type", ""),
                "nullable": col.get("nullable", True),
                "pending":  True,
            }
            if col.get("pii"):
                merged["pii"] = True
            if col.get("sensitivity"):
                merged["sensitivity"] = col["sensitivity"]
            if col.get("description"):
                merged["description"] = col["description"]
            result.append(merged)
    return result or None


def _bump_patch(version: str) -> str:
    """Øk patch-versjon: '1.2.3' → '1.2.4'. Returnerer '1.0.1' ved feil."""
    try:
        major, minor, patch = version.split(".")
        return f"{major}.{minor}.{int(patch) + 1}"
    except Exception:
        return "1.0.1"


_BROWSE_BUCKETS = ["gold", "silver", "analytics"]


def _browse_path(bucket: str, prefix: str) -> list[dict]:
    """
    List ett nivå under `prefix` i `bucket`.
    Returnerer mapper og Delta-tabeller (detektert via _delta_log/).
    """
    s3     = _s3_client()
    prefix = prefix.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    # Samle alle common prefixes i én gjennomgang
    paginator      = s3.get_paginator("list_objects_v2")
    all_prefixes   = []
    delta_parents  = set()

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            sub = cp["Prefix"]
            all_prefixes.append(sub)
            if sub.endswith("_delta_log/"):
                delta_parents.add(sub[: -len("_delta_log/")])

    entries = []
    for sub in all_prefixes:
        name = sub[len(prefix):].rstrip("/")
        if name == "_delta_log":
            continue
        is_delta  = sub in delta_parents or _is_delta_table(bucket, sub)
        full_path = f"s3://{bucket}/{sub.rstrip('/')}"
        col_count = None
        if is_delta:
            schema    = _safe_schema(full_path)
            col_count = len(schema) if schema else None
        entries.append({
            "name":      name,
            "path":      full_path,
            "prefix":    sub,
            "bucket":    bucket,
            "type":      "delta" if is_delta else "folder",
            "col_count": col_count,
        })

    return sorted(entries, key=lambda e: (e["type"] == "folder", e["name"]))


def _is_delta_table(bucket: str, prefix: str) -> bool:
    """Sjekk om prefix inneholder _delta_log/."""
    try:
        s3   = _s3_client()
        resp = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix + "_delta_log/",
            MaxKeys=1,
        )
        return resp.get("KeyCount", 0) > 0
    except Exception:
        return False


def _set_auth_cookies(response, access_token: str, refresh_token: str) -> None:
    response.set_cookie("access_token",  access_token,  httponly=True, samesite="lax", max_age=1800)
    response.set_cookie("refresh_token", refresh_token, httponly=True, samesite="lax", max_age=604800)


def _clear_auth_cookies(response) -> None:
    response.delete_cookie("access_token")
    response.delete_cookie("refresh_token")


# ── Notebook-generering ────────────────────────────────────────────────────────

def _nb_cell(cell_type: str, source: str | list[str], **extra) -> dict:
    if isinstance(source, str):
        source = source.splitlines(keepends=True)
    base = {"cell_type": cell_type, "metadata": {}, "source": source}
    if cell_type == "code":
        base.update({"execution_count": None, "outputs": []})
    return {**base, **extra}


def _generate_product_notebook(manifest: dict, quality: dict | None, schema: list | None) -> dict:
    """Generer en komplett analyseklar .ipynb for ett dataprodukt."""
    pid  = manifest["id"]
    name = manifest["name"]
    cols = ", ".join(f"`{c['name']}`" for c in (schema or [])[:8])
    col_extra = f" … (+{len(schema)-8} til)" if schema and len(schema) > 8 else ""

    q_score = quality.get("score_pct") if quality else None
    q_line  = f"**Kvalitetsscore:** {q_score}%" if q_score is not None else "_Ingen kvalitetsresultater ennå._"

    cells = [
        _nb_cell("markdown", [
            f"# {name}\n\n",
            f"| Felt | Verdi |\n|------|-------|\n",
            f"| **ID** | `{pid}` |\n",
            f"| **Domene** | {manifest.get('domain','')} |\n",
            f"| **Eier** | {manifest.get('owner','')} |\n",
            f"| **Format** | {manifest.get('format','delta')} |\n",
            f"| **Tilgang** | {manifest.get('access','public')} |\n\n",
            f"{manifest.get('description','')}\n\n",
            f"**Kolonner:** {cols}{col_extra}\n\n",
            f"{q_line}\n",
        ]),
        _nb_cell("code", [
            "import sys\n",
            'sys.path.insert(0, "/home/spark/jobs")\n',
            "\n",
            "from slettix_client import get_product, get_manifest, get_quality, get_sla\n",
            "\n",
            f'PRODUCT_ID = "{pid}"\n',
        ]),
        _nb_cell("code", [
            "df = get_product(PRODUCT_ID)\n",
            "print(f'Lastet {len(df)} rader, {len(df.columns)} kolonner')\n",
            "df.head()",
        ]),
        _nb_cell("code", [
            "# Datakvalitet og SLA\n",
            "quality = get_quality(PRODUCT_ID)\n",
            "sla     = get_sla(PRODUCT_ID)\n",
            "print(f\"Kvalitet : {quality.get('score_pct')}% ({quality.get('passed')}/{quality.get('total_expectations')} forventninger)\")\n",
            "print(f\"SLA      : {'OK' if sla.get('compliant') else 'BRUDD'} — {sla.get('hours_since_update')}t siden oppdatering\")",
        ]),
        _nb_cell("code", [
            "# Grunnleggende statistikk\n",
            "df.describe(include='all')",
        ]),
        _nb_cell("markdown", ["## Analyse\n\nLegg til din analyse her."]),
        _nb_cell("code", ["# Din analyse her\n"]),
    ]

    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.11.0"},
        },
        "cells": cells,
    }


def _generate_multi_notebook(manifests: list[dict]) -> dict:
    """Generer en sammenstillings-notebook for flere produkter."""
    names = ", ".join(m["name"] for m in manifests)
    cells = [
        _nb_cell("markdown", [
            f"# Analytisk notebook — {names}\n\n",
            f"Generert: {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n",
        ]),
        _nb_cell("code", [
            "import sys\n",
            'sys.path.insert(0, "/home/spark/jobs")\n',
            "from slettix_client import get_product\n",
            "import pandas as pd\n",
        ]),
    ]

    var_names = []
    for m in manifests:
        var = re.sub(r"[^a-z0-9]", "_", m["id"].lower())
        var_names.append((var, m))
        cells.append(_nb_cell("markdown", [f"## {m['name']}\n"]))
        cells.append(_nb_cell("code", [
            f'df_{var} = get_product("{m["id"]}")\n',
            f'print(f"{{len(df_{var})}} rader, {{len(df_{var}.columns)}} kolonner")\n',
            f"df_{var}.head()",
        ]))

    # Sammenstillingscelle
    join_comment = "# Eksempel: slå sammen på felles nøkkel\n# result = df_{}.merge(df_{}, on='id')\n".format(
        var_names[0][0], var_names[1][0]
    ) if len(var_names) >= 2 else "# Legg til sammenstilling her\n"

    cells.append(_nb_cell("markdown", ["## Sammenstilling\n"]))
    cells.append(_nb_cell("code", [join_comment]))

    source_ids = [m["id"] for m in manifests]
    cells.append(_nb_cell("markdown", ["## Publiser som analytisk dataprodukt\n"]))
    cells.append(_nb_cell("code", [
        "from slettix_client import publish_analytical\n",
        "\n",
        "# publish_analytical(\n",
        "#     df=result,                          # DataFrame som skal publiseres\n",
        f'#     product_id="analytics.mitt_produkt",\n',
        '#     name="Mitt analytiske produkt",\n',
        '#     description="Beskrivelse av produktet",\n',
        '#     domain="analytics",\n',
        '#     owner="mitt-team",\n',
        f'#     source_products={json.dumps(source_ids)},\n',
        "# )\n",
    ]))

    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.11.0"},
        },
        "cells": cells,
    }


def _notebook_path(filename: str) -> pathlib.Path:
    return pathlib.Path(NOTEBOOKS_DIR) / filename


def _safe_filename(product_id: str) -> str:
    return "product_" + re.sub(r"[^a-zA-Z0-9_-]", "_", product_id) + ".ipynb"


def _jupyter_open_url(filename: str) -> str:
    return f"{JUPYTER_EXTERNAL_URL}/lab/tree/{filename}"


def _push_to_jupyter(filename: str, nb: dict) -> None:
    """Push notebook til Jupyter Contents API (intern URL) så filen dukker opp umiddelbart."""
    import httpx, logging
    try:
        _HTTPX_CLIENT.put(
            f"{JUPYTER_URL}/api/contents/{filename}",
            json={"type": "notebook", "content": nb},
            timeout=10,
        )
    except Exception as exc:
        logging.getLogger(__name__).warning("Kunne ikke pushe notebook til Jupyter: %s", exc)


# ── DAG-generering ─────────────────────────────────────────────────────────────

_SCHEDULE_LABELS = {
    "@hourly":   "Hver time",
    "@daily":    "Daglig (kl. 00:00)",
    "@weekly":   "Ukentlig (mandag)",
    "@monthly":  "Månedlig (1. i måneden)",
    "manual":    "Manuell (ingen automatisk kjøring)",
}


def _generate_analytical_notebook(
    manifests: list[dict],
    product_id: str,
    name: str,
    description: str,
    domain: str,
    owner: str,
    source_ids: list[str],
    tags: list[str],
    freshness_hours: int | None,
    access: str,
) -> dict:
    """Genererer en analytisk notebook med loading, transformasjon og publish_analytical."""
    var_names = []
    load_cells = []
    for m in manifests:
        var = re.sub(r"[^a-z0-9]", "_", m["id"].lower())
        var_names.append(var)
        load_cells.append(_nb_cell("code", [
            f'# Last {m["name"]}\n',
            f'df_{var} = get_product("{m["id"]}")\n',
            f'print(f"{{len(df_{var})}} rader, {{len(df_{var}.columns)}} kolonner")\n',
            f"df_{var}.head()",
        ]))

    join_example = ""
    if len(var_names) >= 2:
        join_example = (
            f"# Eksempel: slå sammen {var_names[0]} og {var_names[1]}\n"
            f"# result = df_{var_names[0]}.merge(df_{var_names[1]}, on='<nøkkel>')\n"
        )

    cells = [
        _nb_cell("markdown", [
            f"# {name}\n\n",
            f"{description}\n\n",
            f"**Domene:** {domain} | **Eier:** {owner}\n\n",
            f"**Kildeprodukter:** {', '.join(f'`{s}`' for s in source_ids)}\n",
        ]),
        _nb_cell("code", [
            "import sys\n",
            "# Støtter både Jupyter- og Airflow-kontekst\n",
            'for _p in ["/home/spark/jobs", "/opt/airflow/spark_jobs"]:\n',
            "    if _p not in sys.path:\n",
            "        sys.path.insert(0, _p)\n",
            "\n",
            "from slettix_client import get_product, publish_analytical\n",
            "import pandas as pd\n",
        ]),
        _nb_cell("markdown", ["## 1. Last kildeprodukter\n"]),
        *load_cells,
        _nb_cell("markdown", ["## 2. Transformasjon\n\nLegg til din analyse her."]),
        _nb_cell("code", [
            join_example or "# Legg til din transformasjon her\n",
            "\n",
            "# result = ...  ← bytt ut med din DataFrame\n",
            f"result = df_{var_names[0]}  # TODO: erstatt med faktisk logikk\n",
        ]),
        _nb_cell("markdown", ["## 3. Publiser analytisk dataprodukt\n"]),
        _nb_cell("code", [
            "publish_analytical(\n",
            "    df=result,\n",
            f'    product_id="{product_id}",\n',
            f'    name="{name}",\n',
            f'    description="{description}",\n',
            f'    domain="{domain}",\n',
            f'    owner="{owner}",\n',
            f"    source_products={json.dumps(source_ids)},\n",
            f'    access="{access}",\n',
            f"    tags={json.dumps(tags)},\n",
            ")\n",
        ]),
    ]

    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.11.0"},
        },
        "cells": cells,
    }


def _generate_airflow_dag(
    dag_id: str,
    product_id: str,
    product_name: str,
    notebook_filename: str,
    schedule: str,
    domain: str,
    owner: str,
) -> str:
    """Genererer en Airflow DAG som kjører notebooken via PapermillOperator."""
    safe_id = re.sub(r"[^a-zA-Z0-9_]", "_", product_id)
    return f'''"""
Airflow DAG — Analytisk dataprodukt: {product_name}
Produkt-ID : {product_id}
Auto-generert av Slettix Analytics Portal
"""
from datetime import datetime
from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator

with DAG(
    dag_id="{dag_id}",
    schedule_interval={repr(schedule) if schedule != "manual" else "None"},
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["analytical", "{domain}"],
    doc_md="""
    Kjører analytisk notebook for **{product_name}** og publiserer resultatet
    som dataprodukt `{product_id}` i Slettix Data Portal.
    """,
) as dag:
    run_notebook = PapermillOperator(
        task_id="run_notebook",
        input_nb="/home/spark/notebooks/{notebook_filename}",
        output_nb="/home/spark/notebooks/output/{safe_id}_{{{{ ds }}}}.ipynb",
        parameters={{"run_date": "{{{{ ds }}}}"}},
    )
'''


# ── Selvbetjent pipeline-infrastruktur (Epic 20) ──────────────────────────────

_PIPELINE_TEMPLATES = {
    "csv_ingest": {
        "label":       "CSV-innlesing",
        "icon":        "bi-file-earmark-spreadsheet",
        "description": "Les CSV-filer fra MinIO raw-bucket og skriv til bronze som Delta-tabell",
        "tags":        ["ingest", "csv", "bronze"],
        "fields": [
            {"name": "dag_id",       "label": "DAG-ID",          "type": "text",   "placeholder": "mitt_domene_csv_ingest", "required": True},
            {"name": "source_path",  "label": "Kilde (S3-sti)",   "type": "text",   "placeholder": "s3a://raw/domene/tabell", "required": True},
            {"name": "target_path",  "label": "Mål (S3-sti)",     "type": "text",   "placeholder": "s3a://bronze/domene/tabell", "required": True},
            {"name": "domain",       "label": "Domene",           "type": "text",   "placeholder": "hr", "required": True},
            {"name": "owner",        "label": "Eier",             "type": "text",   "placeholder": "teamet-ditt", "required": True},
            {"name": "schedule",     "label": "Kjøreplan",        "type": "select", "options": ["@daily","@weekly","@monthly","None"], "required": True},
            {"name": "retries",      "label": "Antall forsøk",    "type": "number", "default": "2"},
        ],
    },
    "api_ingest": {
        "label":       "API-innlesing",
        "icon":        "bi-cloud-download",
        "description": "Hent data fra et REST API og lagre til raw/bronze som Delta-tabell",
        "tags":        ["ingest", "api", "bronze"],
        "fields": [
            {"name": "dag_id",       "label": "DAG-ID",           "type": "text",   "placeholder": "mitt_api_ingest", "required": True},
            {"name": "api_url",      "label": "API-URL",          "type": "text",   "placeholder": "https://api.eksempel.no/data", "required": True},
            {"name": "api_headers",  "label": "Headers (JSON)",   "type": "text",   "placeholder": '{"Authorization": "Bearer TOKEN"}', "required": False},
            {"name": "target_path",  "label": "Mål (S3-sti)",     "type": "text",   "placeholder": "s3a://bronze/domene/tabell", "required": True},
            {"name": "domain",       "label": "Domene",           "type": "text",   "placeholder": "hr", "required": True},
            {"name": "owner",        "label": "Eier",             "type": "text",   "placeholder": "teamet-ditt", "required": True},
            {"name": "schedule",     "label": "Kjøreplan",        "type": "select", "options": ["@daily","@weekly","@monthly","None"], "required": True},
            {"name": "retries",      "label": "Antall forsøk",    "type": "number", "default": "2"},
        ],
    },
    "silver_transform": {
        "label":       "Silver-transformasjon",
        "icon":        "bi-funnel",
        "description": "Rens og valider bronze-data til silver med konfigurerbare transformasjonsregler",
        "tags":        ["transform", "silver"],
        "fields": [
            {"name": "dag_id",       "label": "DAG-ID",           "type": "text",   "placeholder": "mitt_silver_transform", "required": True},
            {"name": "config_path",  "label": "Konfig-sti",       "type": "text",   "placeholder": "/opt/airflow/spark_conf/silver/domene/tabell.json", "required": True},
            {"name": "domain",       "label": "Domene",           "type": "text",   "placeholder": "hr", "required": True},
            {"name": "owner",        "label": "Eier",             "type": "text",   "placeholder": "teamet-ditt", "required": True},
            {"name": "schedule",     "label": "Kjøreplan",        "type": "select", "options": ["@daily","@weekly","@monthly","None"], "required": True},
            {"name": "retries",      "label": "Antall forsøk",    "type": "number", "default": "2"},
        ],
    },
    "gold_aggregate": {
        "label":       "Gold-aggregering",
        "icon":        "bi-star",
        "description": "Aggreger silver-data til gold-tabeller med konfigurerbare regler",
        "tags":        ["aggregate", "gold"],
        "fields": [
            {"name": "dag_id",       "label": "DAG-ID",           "type": "text",   "placeholder": "mitt_gold_aggregat", "required": True},
            {"name": "config_path",  "label": "Konfig-sti",       "type": "text",   "placeholder": "/opt/airflow/spark_conf/gold/domene/tabell.json", "required": True},
            {"name": "domain",       "label": "Domene",           "type": "text",   "placeholder": "hr", "required": True},
            {"name": "owner",        "label": "Eier",             "type": "text",   "placeholder": "teamet-ditt", "required": True},
            {"name": "schedule",     "label": "Kjøreplan",        "type": "select", "options": ["@daily","@weekly","@monthly","None"], "required": True},
            {"name": "retries",      "label": "Antall forsøk",    "type": "number", "default": "2"},
        ],
    },
    "scheduled_notebook": {
        "label":       "Planlagt notebook",
        "icon":        "bi-journal-code",
        "description": "Kjør en Jupyter notebook på en tidsplan med parametere via Papermill",
        "tags":        ["notebook", "analytical"],
        "fields": [
            {"name": "dag_id",         "label": "DAG-ID",           "type": "text",   "placeholder": "mitt_notebook_dag", "required": True},
            {"name": "notebook_path",  "label": "Notebook-sti",     "type": "text",   "placeholder": "/home/spark/notebooks/min_notebook.ipynb", "required": True},
            {"name": "domain",         "label": "Domene",           "type": "text",   "placeholder": "analytics", "required": True},
            {"name": "owner",          "label": "Eier",             "type": "text",   "placeholder": "teamet-ditt", "required": True},
            {"name": "schedule",       "label": "Kjøreplan",        "type": "select", "options": ["@daily","@weekly","@monthly","None"], "required": True},
            {"name": "retries",        "label": "Antall forsøk",    "type": "number", "default": "2"},
        ],
    },
    "kafka_idp": {
        "label":       "Kafka → Bronze (IDP Streaming)",
        "icon":        "bi-lightning-charge",
        "description": "Real-time streaming fra Kafka-topics til Bronze Delta-tabell. "
                       "CloudEvent-konvolutten pakkes ut automatisk; payload lagres som JSON "
                       "slik at Silver-jobber kan transformere domenespesifikke felt. "
                       "Spinner opp en kontinuerlig Spark Streaming-pod via Kubernetes.",
        "tags":        ["kafka", "streaming", "bronze", "idp"],
        "streaming":   True,
        "fields": [
            {"name": "dag_id",          "label": "DAG-ID",                    "type": "text",   "placeholder": "mitt_domene_kafka_idp",                   "required": True},
            {"name": "kafka_topics",    "label": "Kafka Topics",              "type": "text",   "placeholder": "mitt.domene.created,mitt.domene.updated",  "required": True},
            {"name": "target_path",     "label": "Mål Bronze (S3-sti)",       "type": "text",   "placeholder": "s3a://bronze/domene/events",              "required": True},
            {"name": "consumer_group",  "label": "Consumer Group",            "type": "text",   "placeholder": "domene-events-idp",                       "required": True},
            {"name": "product_id",      "label": "Produkt-ID",                "type": "text",   "placeholder": "domene.events",                           "required": True},
            {"name": "product_name",    "label": "Produktnavn",               "type": "text",   "placeholder": "Domene — Events (IDP)",                   "required": True},
            {"name": "description",     "label": "Beskrivelse",               "type": "text",   "placeholder": "IDP som konsumerer hendelser fra ...",     "required": False},
            {"name": "domain",          "label": "Domene",                    "type": "text",   "placeholder": "hr",                                       "required": True},
            {"name": "owner",           "label": "Eier",                      "type": "text",   "placeholder": "teamet-ditt",                              "required": True},
            {"name": "trigger_seconds", "label": "Trigger-intervall (sek)",   "type": "number", "default": "5"},
            {"name": "freshness_hours", "label": "SLA: Ferskhet (timer)",     "type": "number", "default": "1"},
            {"name": "partition_by",    "label": "Partisjonér på",            "type": "text",   "placeholder": "event_type,event_date",                    "required": False},
        ],
    },
}


def _generate_dag_from_template(template_id: str, config: dict) -> str:
    """Generer Airflow DAG-kode fra mal og konfigurasjon."""
    dag_id   = config.get("dag_id", "generated_dag")
    domain   = config.get("domain", "default")
    owner    = config.get("owner", "slettix")
    schedule = config.get("schedule", "None")
    retries  = int(config.get("retries", 2))

    _SPARK_CONF_BLOCK = """\
SPARK_CONF = {
    "spark.sql.extensions":                       "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog":            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.endpoint":               "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key":             "admin",
    "spark.hadoop.fs.s3a.secret.key":             "changeme",
    "spark.hadoop.fs.s3a.path.style.access":      "true",
    "spark.hadoop.fs.s3a.impl":                   "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.sql.shuffle.partitions":               "8",
    "spark.driver.host":                          "airflow-scheduler",
    "spark.driver.bindAddress":                   "0.0.0.0",
}
DELTA_JARS = ",".join([
    "io.delta:delta-spark_2.12:3.2.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])"""

    _header = f'''"""
Airflow DAG — {_PIPELINE_TEMPLATES[template_id]["label"]}: {dag_id}
Mal       : {template_id}
Domene    : {domain}
Eier      : {owner}
Auto-generert av Slettix Analytics Portal
"""
from datetime import datetime, timedelta
from airflow import DAG
'''
    _default_args = f'''
default_args = {{
    "owner":                    "{owner}",
    "retries":                  {retries},
    "retry_delay":              timedelta(minutes=3),
    "retry_exponential_backoff": True,
}}

'''
    _dag_open = f'''with DAG(
    dag_id="{dag_id}",
    schedule={repr(schedule) if schedule != "None" else "None"},
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags={repr(["generated", domain] + _PIPELINE_TEMPLATES[template_id]["tags"])},
) as dag:
'''

    if template_id == "csv_ingest":
        source = config.get("source_path", "s3a://raw/data")
        target = config.get("target_path", "s3a://bronze/data")
        body = f'''from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

{_SPARK_CONF_BLOCK}

{_default_args}
{_dag_open}
    ingest = SparkSubmitOperator(
        task_id="csv_to_bronze",
        application="/opt/airflow/spark_jobs/ingest_to_bronze.py",
        conn_id="spark_default",
        packages=DELTA_JARS,
        conf=SPARK_CONF,
        application_args=[
            "--source", "{source}",
            "--target", "{target}",
            "--format", "csv",
            "--ingestion-date", "{{{{ ds }}}}",
        ],
        name="{dag_id}",
        execution_timeout=timedelta(minutes=15),
    )
'''

    elif template_id == "api_ingest":
        api_url     = config.get("api_url", "https://api.example.com/data")
        api_headers = config.get("api_headers", "{}")
        target      = config.get("target_path", "s3a://bronze/data")
        body = f'''import json
from airflow.operators.python import PythonOperator

{_default_args}

def _fetch_api(**context):
    import requests
    import pyarrow as pa
    import pandas as pd
    from deltalake.writer import write_deltalake

    headers = json.loads('{api_headers}') if '{api_headers}' else {{}}
    resp = requests.get("{api_url}", headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict):
        data = [data]
    df = pd.DataFrame(data)
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
    storage_options = {{
        "AWS_ENDPOINT_URL":           "http://minio:9000",
        "AWS_ACCESS_KEY_ID":          "admin",
        "AWS_SECRET_ACCESS_KEY":      "changeme",
        "AWS_ALLOW_HTTP":             "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }}
    write_deltalake("{target}", arrow_table, mode="overwrite", storage_options=storage_options)
    context["task_instance"].log.info(f"Skrev {{len(df)}} rader til {target}")

{_dag_open}
    fetch = PythonOperator(
        task_id="api_fetch",
        python_callable=_fetch_api,
        execution_timeout=timedelta(minutes=15),
    )
'''

    elif template_id == "silver_transform":
        config_path = config.get("config_path", "/opt/airflow/spark_conf/silver/data.json")
        body = f'''from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

{_SPARK_CONF_BLOCK}

{_default_args}
{_dag_open}
    transform = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="/opt/airflow/spark_jobs/transform_bronze_to_silver.py",
        conn_id="spark_default",
        packages=DELTA_JARS,
        conf=SPARK_CONF,
        application_args=["--config", "{config_path}"],
        name="{dag_id}",
        execution_timeout=timedelta(minutes=20),
    )
'''

    elif template_id == "gold_aggregate":
        config_path = config.get("config_path", "/opt/airflow/spark_conf/gold/data.json")
        body = f'''from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

{_SPARK_CONF_BLOCK}

{_default_args}
{_dag_open}
    aggregate = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="/opt/airflow/spark_jobs/build_gold_table.py",
        conn_id="spark_default",
        packages=DELTA_JARS,
        conf=SPARK_CONF,
        application_args=["--config", "{config_path}"],
        name="{dag_id}",
        execution_timeout=timedelta(minutes=20),
    )
'''

    elif template_id == "scheduled_notebook":
        nb_path  = config.get("notebook_path", "/home/spark/notebooks/notebook.ipynb")
        safe_id  = re.sub(r"[^a-zA-Z0-9_]", "_", dag_id)
        body = f'''from airflow.providers.papermill.operators.papermill import PapermillOperator

{_default_args}
{_dag_open}
    run_notebook = PapermillOperator(
        task_id="run_notebook",
        input_nb="{nb_path}",
        output_nb="/home/spark/notebooks/output/{safe_id}_{{{{ ds }}}}.ipynb",
        parameters={{"run_date": "{{{{ ds }}}}"}},
        execution_timeout=timedelta(minutes=30),
    )
'''
    elif template_id == "kafka_idp":
        kafka_topics    = config.get("kafka_topics", "mitt.domene.events")
        target_path     = config.get("target_path", "s3a://bronze/domene/events")
        consumer_group  = config.get("consumer_group", f"{dag_id}-cg")
        product_id      = config.get("product_id", dag_id.replace("_", "."))
        product_name    = config.get("product_name", dag_id)
        description     = config.get("description", f"IDP for {kafka_topics}")
        trigger_seconds = int(config.get("trigger_seconds", 5))
        freshness_hours = int(config.get("freshness_hours", 1))
        partition_by    = config.get("partition_by", "event_type,event_date")
        checkpoint_path = f"s3a://checkpoints/{dag_id}"
        spark_app_name  = re.sub(r"[^a-z0-9-]", "-", dag_id.lower())

        body = f'''"""
Airflow DAG — Kafka IDP: {dag_id}
Mal: kafka_idp
Domene: {domain}
Eier: {owner}
Topics: {kafka_topics}
Target: {target_path}
Auto-generert av Slettix Analytics Portal
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka.customermaster.svc.cluster.local:9092",
)
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "http://minio.slettix-analytics.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "changeme")
PORTAL_URL       = os.environ.get("PORTAL_URL",       "http://dataportal.slettix-analytics.svc.cluster.local:8090")
PORTAL_API_KEY   = os.environ.get("PORTAL_API_KEY",   "dev-key-change-me")

KAFKA_TOPICS    = "{kafka_topics}"
TARGET_PATH     = "{target_path}"
CHECKPOINT_PATH = "{checkpoint_path}"
CONSUMER_GROUP  = "{consumer_group}"
PRODUCT_ID      = "{product_id}"
PRODUCT_NAME    = "{product_name}"
DOMAIN          = "{domain}"
OWNER           = "{owner}"
TRIGGER_SECONDS = {trigger_seconds}
FRESHNESS_HOURS = {freshness_hours}
PARTITION_BY    = "{partition_by}"

SPARK_APP_NAME  = "{spark_app_name}-streaming"
SPARK_NAMESPACE = "slettix-analytics"
SPARK_NS        = SPARK_NAMESPACE


def deploy_streaming_job(**context):
    """
    Oppretter eller oppdaterer SparkApplication-ressursen i Kubernetes.
    Spark Operator holder jobben i live med restartPolicy: Always.
    Idempotent — trygt å kjøre gjentatte ganger.
    """
    log = context["task_instance"].log

    manifest = {{
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {{
            "name": SPARK_APP_NAME,
            "namespace": SPARK_NAMESPACE,
            "labels": {{
                "app": SPARK_APP_NAME,
                "app.kubernetes.io/part-of": "slettix-analytics",
                "app.kubernetes.io/component": "idp-streaming",
                "app.kubernetes.io/managed-by": "pipeline-builder",
            }},
            "annotations": {{
                "slettix.io/product-id": PRODUCT_ID,
                "slettix.io/kafka-topics": KAFKA_TOPICS,
            }},
        }},
        "spec": {{
            "type": "Python",
            "mode": "cluster",
            "image": "slettix-analytics/spark:3.5.8",
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": "local:///opt/spark/jobs/kafka_to_bronze.py",
            "arguments": [
                "--topics",         KAFKA_TOPICS,
                "--target-path",    TARGET_PATH,
                "--checkpoint-path",CHECKPOINT_PATH,
                "--consumer-group", CONSUMER_GROUP,
                "--trigger-seconds",str(TRIGGER_SECONDS),
                "--partition-by",   PARTITION_BY,
                "--starting-offsets","latest",
            ],
            "sparkVersion": "3.5.8",
            "restartPolicy": {{
                "type": "Always",
                "onFailureRetries": 10,
                "onFailureRetryInterval": 30,
                "onSubmissionFailureRetries": 5,
                "onSubmissionFailureRetryInterval": 20,
            }},
            "sparkConf": {{
                "spark.sql.extensions":                       "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog":            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.hadoop.fs.s3a.endpoint":               MINIO_ENDPOINT,
                "spark.hadoop.fs.s3a.path.style.access":      "true",
                "spark.hadoop.fs.s3a.impl":                   "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
                "spark.sql.shuffle.partitions":               "4",
                "spark.databricks.delta.schema.autoMerge.enabled": "true",
            }},
            "driver": {{
                "cores": 1,
                "memory": "512m",
                "memoryOverhead": "128m",
                "serviceAccount": "spark",
                "env": [
                    {{"name": "KAFKA_BOOTSTRAP_SERVERS", "value": KAFKA_BOOTSTRAP_SERVERS}},
                    {{"name": "AWS_ACCESS_KEY_ID",
                      "valueFrom": {{"secretKeyRef": {{"name": "slettix-credentials", "key": "minio-root-user"}}}}}},
                    {{"name": "AWS_SECRET_ACCESS_KEY",
                      "valueFrom": {{"secretKeyRef": {{"name": "slettix-credentials", "key": "minio-root-password"}}}}}},
                ],
            }},
            "executor": {{
                "instances": 1,
                "cores": 1,
                "memory": "512m",
                "memoryOverhead": "128m",
                "env": [
                    {{"name": "KAFKA_BOOTSTRAP_SERVERS", "value": KAFKA_BOOTSTRAP_SERVERS}},
                    {{"name": "AWS_ACCESS_KEY_ID",
                      "valueFrom": {{"secretKeyRef": {{"name": "slettix-credentials", "key": "minio-root-user"}}}}}},
                    {{"name": "AWS_SECRET_ACCESS_KEY",
                      "valueFrom": {{"secretKeyRef": {{"name": "slettix-credentials", "key": "minio-root-password"}}}}}},
                ],
            }},
        }},
    }}

    try:
        from kubernetes import client as k8s, config as k8s_config
        try:
            k8s_config.load_incluster_config()
        except Exception:
            k8s_config.load_kube_config()

        custom_api = k8s.CustomObjectsApi()
        grp, ver, plural = "sparkoperator.k8s.io", "v1beta2", "sparkapplications"

        try:
            existing = custom_api.get_namespaced_custom_object(
                group=grp, version=ver, namespace=SPARK_NAMESPACE,
                plural=plural, name=SPARK_APP_NAME,
            )
            manifest["metadata"]["resourceVersion"] = existing["metadata"]["resourceVersion"]
            custom_api.replace_namespaced_custom_object(
                group=grp, version=ver, namespace=SPARK_NAMESPACE,
                plural=plural, name=SPARK_APP_NAME, body=manifest,
            )
            log.info(f"SparkApplication {{SPARK_APP_NAME}} oppdatert")
        except k8s.ApiException as exc:
            if exc.status == 404:
                custom_api.create_namespaced_custom_object(
                    group=grp, version=ver, namespace=SPARK_NAMESPACE,
                    plural=plural, body=manifest,
                )
                log.info(f"SparkApplication {{SPARK_APP_NAME}} opprettet")
            else:
                raise
    except Exception as exc:
        log.error(f"Kunne ikke deploye SparkApplication: {{exc}}")
        raise


def register_product(**context):
    """
    Registrerer dataprodukt-manifestet i Delta-registeret.
    Idempotent — oppdaterer hvis produktet allerede finnes.
    """
    import json
    log = context["task_instance"].log
    sys.path.insert(0, "/opt/airflow/jobs")

    manifest = {{
        "id":          PRODUCT_ID,
        "name":        PRODUCT_NAME,
        "domain":      DOMAIN,
        "owner":       OWNER,
        "version":     "1.0.0",
        "description": "{description}",
        "source_path": TARGET_PATH.replace("s3a://", "s3://"),
        "format":      "delta",
        "dag_id":      "{dag_id}",
        "product_type":"source",
        "access":      "restricted",
        "quality_sla": {{"freshness_hours": FRESHNESS_HOURS}},
        "tags":        ["kafka", "streaming", "idp", "bronze", DOMAIN],
        "contract": {{
            "slo": {{"freshness_hours": FRESHNESS_HOURS}},
            "schema_compatibility": "FORWARD",
        }},
        "schema": [
            {{"name": "event_id",       "type": "string", "nullable": True,  "pii": False, "description": "CloudEvent eventId"}},
            {{"name": "event_type",     "type": "string", "nullable": True,  "pii": False, "description": "CloudEvent eventType"}},
            {{"name": "event_timestamp","type": "string", "nullable": True,  "pii": False, "description": "CloudEvent occurredAt"}},
            {{"name": "event_source",   "type": "string", "nullable": True,  "pii": False, "description": "CloudEvent source"}},
            {{"name": "event_version",  "type": "string", "nullable": True,  "pii": False, "description": "CloudEvent version"}},
            {{"name": "payload_json",   "type": "string", "nullable": True,  "pii": True,  "sensitivity": "high",
              "description": "Rå JSON-payload — domenespesifikke felt transformeres i Silver"}},
            {{"name": "kafka_topic",    "type": "string", "nullable": False, "pii": False, "description": "Kafka-topic meldingen ble lest fra"}},
            {{"name": "kafka_partition","type": "integer","nullable": False, "pii": False}},
            {{"name": "kafka_offset",   "type": "long",   "nullable": False, "pii": False}},
            {{"name": "kafka_timestamp","type": "timestamp","nullable": True,"pii": False}},
            {{"name": "_processed_at",  "type": "timestamp","nullable": False,"pii": False, "description": "Prosesseringstidspunkt (IDP)"}},
            {{"name": "_raw_value",     "type": "string", "nullable": True,  "pii": True,  "sensitivity": "high",
              "description": "Rå Kafka-meldingsverdi (JSON-streng)"}},
            {{"name": "event_date",     "type": "date",   "nullable": True,  "pii": False, "description": "Partisjoneringskolonne (avledet av event_timestamp)"}},
        ],
    }}

    try:
        from registry import register
        register(manifest)
        log.info(f"Produkt {{PRODUCT_ID}} registrert i Delta-registeret")
    except Exception as exc:
        log.warning(f"Registrering i Delta-registeret feilet: {{exc}}")

    # Patch dag_id i portalen
    try:
        import requests
        requests.patch(
            f"{{PORTAL_URL}}/api/products/{{PRODUCT_ID}}",
            json={{"dag_id": "{dag_id}"}},
            headers={{"X-API-Key": PORTAL_API_KEY}},
            timeout=10,
        )
        log.info(f"dag_id patchet i portalen for {{PRODUCT_ID}}")
    except Exception as exc:
        log.warning(f"Kunne ikke patche dag_id i portalen: {{exc}}")


default_args = {{
    "owner":       "{owner}",
    "retries":     1,
    "retry_delay": timedelta(minutes=2),
}}

with DAG(
    dag_id="{dag_id}",
    description="Kafka IDP: {kafka_topics} → {target_path}",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags={repr(["generated", "kafka", "streaming", "idp", domain])},
    doc_md=__doc__,
) as dag:

    deploy = PythonOperator(
        task_id="deploy_streaming_job",
        python_callable=deploy_streaming_job,
        execution_timeout=timedelta(minutes=5),
    )

    register = PythonOperator(
        task_id="register_product",
        python_callable=register_product,
        execution_timeout=timedelta(minutes=5),
    )

    deploy >> register
'''

    else:
        raise ValueError(f"Ukjent mal: {template_id}")

    return _header + body


def _save_pipeline_config(dag_id: str, template_id: str, config: dict) -> None:
    """Lagre pipeline-konfigurasjon til MinIO for versjonering og rollback."""
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )
    now     = datetime.now(tz=timezone.utc).isoformat()
    payload = {"dag_id": dag_id, "template_id": template_id, "config": config, "saved_at": now}
    # Current config
    s3.put_object(
        Bucket="gold",
        Key=f"pipeline_configs/{dag_id}/current.json",
        Body=json.dumps(payload, indent=2).encode(),
        ContentType="application/json",
    )
    # Append to history
    history = []
    try:
        obj = s3.get_object(Bucket="gold", Key=f"pipeline_configs/{dag_id}/history.json")
        history = json.loads(obj["Body"].read())
    except ClientError:
        pass
    history.insert(0, payload)
    history = history[:10]  # maks 10 versjoner
    s3.put_object(
        Bucket="gold",
        Key=f"pipeline_configs/{dag_id}/history.json",
        Body=json.dumps(history, indent=2).encode(),
        ContentType="application/json",
    )


def _load_pipeline_config(dag_id: str) -> dict | None:
    """Last gjeldende pipeline-konfigurasjon fra MinIO."""
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
        )
        obj = s3.get_object(Bucket="gold", Key=f"pipeline_configs/{dag_id}/current.json")
        return json.loads(obj["Body"].read())
    except ClientError:
        return None


def _load_pipeline_history(dag_id: str) -> list[dict]:
    """Last versjonshistorikk for en pipeline."""
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
        )
        obj = s3.get_object(Bucket="gold", Key=f"pipeline_configs/{dag_id}/history.json")
        return json.loads(obj["Body"].read())
    except ClientError:
        return []


_ERROR_HINTS = [
    ("NoSuchKey",          "Kildefil ikke funnet i MinIO. Sjekk at S3-stien er korrekt og at filen er lastet opp."),
    ("Connection refused", "Kunne ikke koble til Spark-master. Sjekk at spark-master-containeren kjører."),
    ("AnalysisException",  "Spark kunne ikke analysere spørringen. Sjekk kolonnenavn og schema i kildedata."),
    ("NoSuchKernel",       "Python-kernel ikke funnet i Airflow. Kjør: pip install ipykernel && python -m ipykernel install --user"),
    ("PermissionError",    "Mangler tilgang. Sjekk PORTAL_API_KEY og tilgangskontroll i portalen."),
    ("PapermillExecutionError", "Notebooken feilet under kjøring. Åpne output-notebooken i Jupyter for å se detaljert feilmelding."),
    ("SparkException",     "Spark-jobb feilet. Sjekk Spark Operator-logger med: kubectl logs -n slettix-analytics -l spark-role=driver"),
    ("FileNotFoundError",  "En fil ble ikke funnet. Sjekk at notebook-stien og konfig-stien eksisterer."),
]

def _diagnose_error(error_msg: str) -> str | None:
    """Returner brukervennlig hint basert på feilmelding."""
    for pattern, hint in _ERROR_HINTS:
        if pattern.lower() in (error_msg or "").lower():
            return hint
    return None


# ── API: Delta Lake-browser ───────────────────────────────────────────────────

@app.get("/api/browser", tags=["browser"], summary="Naviger MinIO og detekter Delta-tabeller")
def api_browser(path: str = ""):
    """
    List mapper og Delta-tabeller på gitt sti.
    path-format: «bucket/prefix» f.eks. «gold/hr» eller bare «gold».
    Støtter gold- og analytics-buckets.
    """
    parts  = path.strip("/").split("/", 1) if path.strip("/") else []
    bucket = parts[0] if parts else ""
    prefix = parts[1] if len(parts) > 1 else ""

    # Rot-nivå: vis tilgjengelige buckets
    if not bucket:
        result = []
        for b in _BROWSE_BUCKETS:
            try:
                children = _browse_path(b, "")
                result.append({"name": b, "path": f"s3://{b}", "bucket": b,
                               "prefix": "", "type": "bucket", "col_count": None,
                               "children": children})
            except Exception:
                pass
        return result

    if bucket not in _BROWSE_BUCKETS:
        raise HTTPException(status_code=400, detail=f"Bucket '{bucket}' er ikke tilgjengelig for browsing")

    try:
        return _browse_path(bucket, prefix)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


# ── API: Jupyter-notebook-generering ──────────────────────────────────────────

@app.post("/api/products/{product_id}/notebook", tags=["jupyter"],
          summary="Generer notebook for ett produkt")
def api_generate_notebook(product_id: str, request: Request, force: bool = False):
    """
    Genererer en .ipynb for produktet og lagrer til notebooks/-mappen.
    Returnerer Jupyter Lab-URL som åpner filen direkte.
    Eksisterende notebook gjenbrukes med mindre ?force=true.
    """
    manifest = _resolve_product(product_id)
    user     = auth.get_current_user(request)
    _require_access(manifest, user, None)

    filename = _safe_filename(product_id)
    nb_path  = _notebook_path(filename)

    if nb_path.exists() and not force:
        return {"filename": filename, "url": _jupyter_open_url(filename), "created": False}

    schema  = manifest.get("delta_schema") or _safe_schema(manifest["source_path"])
    quality = _safe_quality(product_id)
    nb      = _generate_product_notebook(manifest, quality, schema)

    pathlib.Path(NOTEBOOKS_DIR).mkdir(parents=True, exist_ok=True)
    nb_path.write_text(json.dumps(nb, ensure_ascii=False, indent=1))
    _push_to_jupyter(filename, nb)

    return {"filename": filename, "url": _jupyter_open_url(filename), "created": True}


@app.post("/api/notebooks/multi", tags=["jupyter"],
          summary="Generer sammenstillings-notebook for flere produkter")
async def api_generate_multi_notebook(request: Request):
    """
    Body: {"product_ids": ["id1", "id2"], "name": "valgfritt-navn"}
    Genererer analytics_<timestamp>.ipynb (eller <name>.ipynb) og lagrer til notebooks/.
    """
    body        = await request.json()
    product_ids = body.get("product_ids", [])
    custom_name = (body.get("name") or "").strip()

    if not product_ids:
        raise HTTPException(status_code=422, detail="product_ids er påkrevd")

    manifests = []
    for pid in product_ids:
        try:
            manifests.append(get(pid))
        except KeyError:
            raise HTTPException(status_code=404, detail=f"Produkt '{pid}' ikke funnet")

    if custom_name:
        safe = re.sub(r"[^a-zA-Z0-9_-]", "_", custom_name)
        filename = safe if safe.endswith(".ipynb") else safe + ".ipynb"
    else:
        ts       = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"analytics_{ts}.ipynb"

    nb      = _generate_multi_notebook(manifests)
    nb_path = _notebook_path(filename)
    pathlib.Path(NOTEBOOKS_DIR).mkdir(parents=True, exist_ok=True)
    nb_path.write_text(json.dumps(nb, ensure_ascii=False, indent=1))
    _push_to_jupyter(filename, nb)

    return {"filename": filename, "url": _jupyter_open_url(filename)}


# ── auth: API ──────────────────────────────────────────────────────────────────

@app.post("/auth/register", tags=["auth"], summary="Selvregistrering")
async def api_register(request: Request):
    data = await request.json()
    username = (data.get("username") or "").strip()
    email    = (data.get("email")    or "").strip()
    password = data.get("password")   or ""
    if not username or not email or not password:
        raise HTTPException(status_code=422, detail="username, email og password er påkrevd")
    if len(password) < 6:
        raise HTTPException(status_code=422, detail="Passord må være minst 6 tegn")
    try:
        user = auth.create_user(username, email, password)
    except Exception as exc:
        if "UNIQUE" in str(exc):
            raise HTTPException(status_code=409, detail="Brukernavn eller e-post er allerede registrert")
        raise HTTPException(status_code=500, detail=str(exc))
    return {"id": user["id"], "username": user["username"], "role": user["role"]}


@app.post("/auth/login", tags=["auth"], summary="Logg inn")
async def api_login(request: Request):
    data     = await request.json()
    username = data.get("username", "")
    password = data.get("password", "")
    user     = auth.get_user_by_username(username)
    if not user or not auth.verify_password(password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Feil brukernavn eller passord")
    access_token  = auth.create_access_token(user["id"], user["username"], user["role"])
    refresh_token = auth.create_refresh_token(user["id"])
    response      = JSONResponse({
        "access_token": access_token,
        "username":     user["username"],
        "role":         user["role"],
    })
    _set_auth_cookies(response, access_token, refresh_token)
    return response


@app.post("/auth/logout", tags=["auth"], summary="Logg ut")
def api_logout(request: Request):
    refresh_token = request.cookies.get("refresh_token")
    if refresh_token:
        auth.revoke_refresh_token(refresh_token)
    response = JSONResponse({"status": "logged_out"})
    _clear_auth_cookies(response)
    return response


@app.post("/auth/refresh", tags=["auth"], summary="Forny access token")
def api_refresh(request: Request):
    refresh_token = request.cookies.get("refresh_token")
    if not refresh_token:
        raise HTTPException(status_code=401, detail="Ingen refresh token")
    result = auth.rotate_refresh_token(refresh_token)
    if not result:
        raise HTTPException(status_code=401, detail="Ugyldig eller utgått refresh token")
    new_refresh, user_id = result
    user = auth.get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=401, detail="Bruker ikke funnet")
    access_token = auth.create_access_token(user["id"], user["username"], user["role"])
    response     = JSONResponse({"access_token": access_token})
    _set_auth_cookies(response, access_token, new_refresh)
    return response


# ── API: dataprodukter ─────────────────────────────────────────────────────────

@app.get("/api/products", tags=["products"], summary="Liste alle dataprodukter")
def api_list_products(request: Request, api_key: str | None = Security(_api_key_scheme)):
    user     = auth.get_current_user(request)
    products = list_all()
    return [
        p for p in products
        if _check_product_access(p, user, api_key)
    ]


@app.post("/api/products", status_code=201, tags=["products"], summary="Registrer et dataprodukt")
def api_register_product(
    manifest: dict,
    request:  Request,
    api_key:  str | None = Security(_api_key_scheme),
):
    user = auth.get_current_user(request)
    if api_key != PORTAL_API_KEY and (not user or user["role"] != "admin"):
        raise HTTPException(status_code=403, detail="Registrering krever admin-rolle eller gyldig API-nøkkel.")

    # Domeneisolasjon: brukere kan bare publisere i sine egne domener (admin kan alt)
    if user and user["role"] != "admin":
        user_domains = auth.get_user_domains(user["id"])
        product_domain = manifest.get("domain", "")
        if user_domains and product_domain and product_domain not in user_domains:
            raise HTTPException(
                status_code=403,
                detail=f"Du har ikke tilgang til å publisere i domenet '{product_domain}'. Kontakt admin for å få domeneprivilegier.",
            )

    required = {"id", "name", "domain", "owner", "version", "source_path", "format"}
    missing  = required - manifest.keys()
    if missing:
        raise HTTPException(status_code=422, detail=f"Manglende felt: {sorted(missing)}")

    product_id = manifest["id"]

    # ── Skjemakompatibilitetssjekk ────────────────────────────────────────
    contract      = manifest.get("contract") or {}
    compat_mode   = contract.get("schema_compatibility", "NONE")
    history       = list_versions(product_id)
    prev_manifest = history[0]["manifest"] if history else {}
    old_schema    = prev_manifest.get("schema")
    new_schema    = manifest.get("schema")

    violations = _check_schema_compatibility(old_schema, new_schema, compat_mode)
    if violations:
        raise HTTPException(
            status_code=409,
            detail={
                "error":             "schema_compatibility_violation",
                "compatibility_mode": compat_mode,
                "violations":         violations,
                "message":            f"Schema-endring bryter {compat_mode}-kompatibilitet. "
                                      f"Øk versjonsnummeret eller endre schema_compatibility.",
            },
        )

    register(manifest)

    # ── Varsle abonnenter ved schema- eller SLO-endring ───────────────────
    if prev_manifest:
        schema_changed = old_schema != new_schema
        old_contract   = prev_manifest.get("contract") or {}
        slo_changed    = old_contract.get("slo") != contract.get("slo")
        if schema_changed or slo_changed:
            changes = []
            if schema_changed:
                changes.append("Schema er endret")
            if slo_changed:
                changes.append("SLO er endret")
            _notify_subscribers(
                product_id,
                subject=f"Endring i dataprodukt: {manifest.get('name', product_id)}",
                body=(
                    f"Produkt `{product_id}` (v{manifest.get('version')}) er oppdatert.\n"
                    + "\n".join(f"• {c}" for c in changes)
                    + f"\nSe detaljer: http://localhost:8090/products/{product_id}"
                ),
            )

    return {"status": "registered", "id": product_id}


@app.get("/api/products/{product_id}/schema", tags=["products"], summary="Delta-tabellens schema")
def api_get_schema(product_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    manifest = _resolve_product(product_id)
    user     = auth.get_current_user(request)
    _require_access(manifest, user, api_key)
    schema = manifest.get("delta_schema") or _safe_schema(manifest["source_path"])
    if schema is None:
        raise HTTPException(status_code=502, detail="Kunne ikke lese schema fra Delta-tabellen")
    return schema


@app.get("/api/products/{product_id}/pipeline", tags=["products"], summary="Siste pipeline-kjøring")
def api_get_pipeline(product_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    manifest = _resolve_product(product_id)
    user     = auth.get_current_user(request)
    _require_access(manifest, user, api_key)
    return _safe_pipeline(manifest.get("dag_id"))


@app.patch("/api/products/{product_id}", tags=["products"], summary="Oppdater manifest-felter")
async def api_patch_product(
    product_id: str,
    request: Request,
    api_key: str | None = Security(_api_key_scheme),
):
    """
    Oppdaterer enkeltfelter i et eksisterende produktmanifest.
    Nyttig for å koble dag_id, oppdatere beskrivelse, tags osv.
    Body: JSON-objekt med feltene som skal oppdateres (f.eks. {"dag_id": "01_slettix_pipeline"}).
    Krever innlogging eller gyldig API-nøkkel.
    """
    user = auth.get_current_user(request)
    if not user and not api_key:
        raise HTTPException(status_code=401, detail="Krever innlogging eller API-nøkkel")

    updates  = await request.json()
    manifest = _resolve_product(product_id)

    # Ikke tillat endring av id
    updates.pop("id", None)
    manifest.update(updates)
    register(manifest)
    return {"updated": list(updates.keys()), "product_id": product_id}


@app.patch("/api/products/{product_id}/subscribe", tags=["products"], summary="Abonner på endringer")
def api_subscribe(product_id: str, request: Request):
    """Registrer innlogget bruker som abonnent på produkt-endringer."""
    user = auth.get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Logg inn for å abonnere")
    _resolve_product(product_id)
    result = auth.subscribe(user["id"], product_id)
    return {"subscribed": True, "product_id": product_id, "created_at": result["created_at"]}


@app.delete("/api/products/{product_id}/subscribe", tags=["products"], summary="Avslutt abonnement")
def api_unsubscribe(product_id: str, request: Request):
    """Fjern innlogget bruker fra abonnentlisten."""
    user = auth.get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Logg inn for å avslutte abonnement")
    auth.unsubscribe(user["id"], product_id)
    return {"subscribed": False, "product_id": product_id}


@app.get("/api/products/{product_id}/subscribers", tags=["products"], summary="List abonnenter (kun eier/admin)")
def api_list_subscribers(product_id: str, request: Request):
    manifest = _resolve_product(product_id)
    user     = auth.get_current_user(request)
    if not user or (user["role"] != "admin" and manifest.get("owner") != user["username"]):
        raise HTTPException(status_code=403, detail="Kun produkteier eller admin kan se abonnenter")
    return auth.list_subscribers(product_id)


# ── API: Pipeline-builder (Epic 20) ───────────────────────────────────────────

@app.post("/api/pipelines/preview", tags=["pipelines"], summary="Forhåndsvis generert DAG-kode")
async def api_pipeline_preview(request: Request):
    """Generer DAG-kode fra mal og konfig uten å skrive til disk."""
    body        = await request.json()
    template_id = body.get("template_id")
    config      = body.get("config", {})
    if template_id not in _PIPELINE_TEMPLATES:
        raise HTTPException(status_code=422, detail=f"Ukjent mal: {template_id}")
    try:
        code = _generate_dag_from_template(template_id, config)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return {"code": code, "dag_id": config.get("dag_id")}


@app.post("/api/pipelines", status_code=201, tags=["pipelines"], summary="Opprett og deploy ny pipeline")
async def api_create_pipeline(request: Request, api_key: str | None = Security(_api_key_scheme)):
    """Generer DAG fra mal, skriv til DAGS_DIR, lagre konfig i MinIO."""
    user = auth.get_current_user(request)
    if not user and api_key != PORTAL_API_KEY:
        raise HTTPException(status_code=401, detail="Krever innlogging eller API-nøkkel")
    body        = await request.json()
    template_id = body.get("template_id")
    config      = body.get("config", {})
    if template_id not in _PIPELINE_TEMPLATES:
        raise HTTPException(status_code=422, detail=f"Ukjent mal: {template_id}")
    dag_id = config.get("dag_id", "").strip()
    if not dag_id or not re.match(r"^[a-zA-Z0-9_]+$", dag_id):
        raise HTTPException(status_code=422, detail="dag_id må kun inneholde bokstaver, tall og understrek")
    try:
        code     = _generate_dag_from_template(template_id, config)
        dag_path = pathlib.Path(DAGS_DIR) / f"{dag_id}.py"
        dag_path.write_text(code, encoding="utf-8")
        _save_pipeline_config(dag_id, template_id, config)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Kunne ikke opprette DAG: {exc}")
    return {"status": "deployed", "dag_id": dag_id, "dag_file": str(dag_path)}


@app.put("/api/pipelines/{dag_id}", tags=["pipelines"], summary="Oppdater og redeploy pipeline")
async def api_update_pipeline(dag_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    """Oppdater config, regenerer DAG og skriv til DAGS_DIR."""
    user = auth.get_current_user(request)
    if not user and api_key != PORTAL_API_KEY:
        raise HTTPException(status_code=401, detail="Krever innlogging eller API-nøkkel")
    body        = await request.json()
    template_id = body.get("template_id")
    config      = body.get("config", {})
    if template_id not in _PIPELINE_TEMPLATES:
        raise HTTPException(status_code=422, detail=f"Ukjent mal: {template_id}")
    try:
        code     = _generate_dag_from_template(template_id, config)
        dag_path = pathlib.Path(DAGS_DIR) / f"{dag_id}.py"
        dag_path.write_text(code, encoding="utf-8")
        _save_pipeline_config(dag_id, template_id, config)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Kunne ikke oppdatere DAG: {exc}")
    return {"status": "redeployed", "dag_id": dag_id}


@app.get("/api/pipelines/{dag_id}/config", tags=["pipelines"], summary="Hent pipeline-konfigurasjon")
def api_get_pipeline_config(dag_id: str):
    """Hent lagret konfigurasjon og historikk for en pipeline."""
    current = _load_pipeline_config(dag_id)
    history = _load_pipeline_history(dag_id)
    return {"current": current, "history": history}


@app.get("/api/pipelines/{dag_id}/status", tags=["pipelines"], summary="Hent Airflow-status for DAG")
def api_get_pipeline_status(dag_id: str):
    """Poll Airflow API for deploy-status og siste kjøring."""
    try:
        resp = _HTTPX_CLIENT.get(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}",
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            timeout=5.0,
        )
        if resp.status_code == 404:
            return {"status": "not_found", "message": "DAG ikke funnet i Airflow ennå — venter på fil-watcher"}
        resp.raise_for_status()
        dag_data = resp.json()
        last_run = _safe_pipeline(dag_id)
        hint     = _diagnose_error(last_run.get("error")) if last_run else None
        return {
            "status":    "active" if not dag_data.get("is_paused") else "paused",
            "dag_id":    dag_id,
            "is_paused": dag_data.get("is_paused"),
            "last_run":  last_run,
            "hint":      hint,
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.get("/api/products/{product_id}/quality", tags=["products"], summary="Siste GE-valideringsresultat")
def api_get_quality(product_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    manifest = _resolve_product(product_id)
    user     = auth.get_current_user(request)
    _require_access(manifest, user, api_key)
    s3  = _s3_client()
    key = f"quality_results/{product_id}/latest.json"
    try:
        obj = s3.get_object(Bucket="gold", Key=key)
        return json.loads(obj["Body"].read())
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            raise HTTPException(status_code=404, detail="Ingen kvalitetsresultater funnet")
        raise HTTPException(status_code=502, detail=str(exc))


@app.get("/api/products/{product_id}/quality/history", tags=["products"], summary="Historisk kvalitetstidsserie")
def api_get_quality_history(product_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    manifest = _resolve_product(product_id)
    user     = auth.get_current_user(request)
    _require_access(manifest, user, api_key)
    return _safe_quality_history(product_id)


@app.get("/api/products/{product_id}/anomalies", tags=["products"], summary="Siste anomalideteksjon")
def api_get_anomalies(product_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    manifest = _resolve_product(product_id)
    user     = auth.get_current_user(request)
    _require_access(manifest, user, api_key)
    result = _safe_anomalies(product_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Ingen anomaliresultater funnet")
    return result


@app.get("/api/products/{product_id}/idp-status", tags=["products"], summary="Streaming IDP-status (SparkApplication)")
def api_get_idp_status(product_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    manifest = _resolve_product(product_id)
    user     = auth.get_current_user(request)
    _require_access(manifest, user, api_key)
    result = _safe_idp_status(manifest)
    if result is None:
        raise HTTPException(status_code=404, detail="Ingen streaming IDP funnet for dette produktet")
    return result


@app.get("/api/products/{product_id}/incidents", tags=["products"], summary="Alle incidents for produktet")
def api_list_incidents(product_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    manifest = _resolve_product(product_id)
    user     = auth.get_current_user(request)
    _require_access(manifest, user, api_key)
    return auth.list_incidents(product_id=product_id)


@app.post("/api/products/{product_id}/incidents", tags=["products"], summary="Opprett incident")
async def api_create_incident(product_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    user = auth.get_current_user(request)
    if not user and api_key != PORTAL_API_KEY:
        raise HTTPException(status_code=401, detail="Krever innlogging eller API-nøkkel")
    _resolve_product(product_id)
    body      = await request.json()
    title     = body.get("title", "").strip()
    if not title:
        raise HTTPException(status_code=422, detail="title er påkrevd")
    created_by = user["username"] if user else "system"
    incident   = auth.create_incident(
        product_id=product_id,
        title=title,
        description=body.get("description", ""),
        severity=body.get("severity", "warning"),
        created_by=created_by,
    )
    return incident


@app.patch("/api/incidents/{incident_id}", tags=["products"], summary="Oppdater incident-status")
async def api_update_incident(incident_id: str, request: Request):
    user = auth.get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Krever innlogging")
    body   = await request.json()
    status = body.get("status", "")
    result = auth.update_incident(incident_id, status, user["username"])
    if not result:
        raise HTTPException(status_code=404, detail="Incident ikke funnet eller ugyldig status")
    # Varsle abonnenter ved resolved
    if status == "resolved":
        _notify_subscribers(
            result["product_id"],
            f"[{result['product_id']}] Incident løst: {result['title']}",
            "",
        )
    return result


# ── API: søk ──────────────────────────────────────────────────────────────────

@app.get("/api/search", tags=["search"], summary="Semantisk søk i produktkatalogen")
def api_search(q: str = "", limit: int = 20, request: Request = None):
    """
    Full-tekst søk over alle produkter. Indekserer navn, beskrivelse,
    domene, eier, tags og kolonnenavn. Returnerer rangerte resultater.
    """
    if not q or not q.strip():
        return []
    products = list_all()
    results  = _search_products(q.strip(), products, limit=limit)

    # Logg søkehendelse (anonymt)
    user = auth.get_current_user(request) if request else None
    uid  = user["id"] if user else None
    for r in results:
        auth.track_usage(r["id"], "search", uid)

    return results


@app.get("/api/products/{product_id}/related", tags=["products"], summary="Relaterte produkter")
def api_get_related(product_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    manifest     = _resolve_product(product_id)
    user         = auth.get_current_user(request)
    _require_access(manifest, user, api_key)
    all_products = list_all()
    related      = _related_products(product_id, manifest, all_products)
    return related


@app.post("/api/products/{product_id}/generate-description", tags=["products"], summary="AI-generert produktbeskrivelse")
async def api_generate_description(product_id: str, request: Request):
    """
    Kaller Claude API med schema og kontekst for å generere en produktbeskrivelse.
    Krever ANTHROPIC_API_KEY og at innlogget bruker er eier eller admin.
    """
    user = auth.get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Krever innlogging")
    manifest = _resolve_product(product_id)
    is_owner = user["role"] == "admin" or manifest.get("owner") == user.get("username")
    if not is_owner:
        raise HTTPException(status_code=403, detail="Kun produkteier eller admin kan generere beskrivelse")
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY er ikke konfigurert")

    # Bygg prompt-kontekst
    schema_text = ""
    if manifest.get("schema"):
        cols = [
            f"  - {c['name']} ({c.get('type','?')})"
            + (f" [PII]" if c.get("pii") else "")
            + (f": {c['description']}" if c.get("description") else "")
            for c in manifest["schema"]
        ]
        schema_text = "Kolonner:\n" + "\n".join(cols)

    prompt = f"""Du er en teknisk forfatter for en data mesh-plattform. Skriv en konsis, norsk produktbeskrivelse for følgende dataprodukt.

Produkt: {manifest.get('name', product_id)}
Domene: {manifest.get('domain', '?')}
Format: {manifest.get('format', '?')}
{schema_text}

Krav til beskrivelsen:
- 2-4 setninger
- Forklar hva produktet inneholder og hvilke bruksscenarier det passer for
- Nevn viktige kolonner ved navn
- Skriv på norsk, faglig men forståelig
- Ikke bruk teknisk jargon unødvendig
- Ikke gjenta produktnavnet i første setning

Returner kun beskrivelsesteksten, ingen markdown-formatering."""

    try:
        import anthropic
        client   = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message  = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        generated = message.content[0].text.strip()
        return {"description": generated}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"AI-generering feilet: {exc}")


@app.post("/api/nl2sql", tags=["search"], summary="Naturlig språk til SQL")
async def api_nl2sql(request: Request):
    """
    Oversett naturlig språk til SQL ved hjelp av Claude API.
    Body: {question: str, product_id: str (valgfritt)}
    """
    user = auth.get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Krever innlogging")
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY er ikke konfigurert")

    body       = await request.json()
    question   = body.get("question", "").strip()
    product_id = body.get("product_id", "").strip()
    if not question:
        raise HTTPException(status_code=422, detail="question er påkrevd")

    schema_ctx = ""
    if product_id:
        try:
            m = get(product_id)
            if m.get("schema"):
                cols = ", ".join(
                    f"{c['name']} {c.get('type','TEXT')}"
                    for c in m["schema"]
                    if not c.get("pii")  # Ikke inkluder PII-kolonner i konteksten
                )
                table_name = product_id.replace(".", "_")
                schema_ctx = f"\nTabell: {table_name} ({cols})"
        except KeyError:
            pass

    prompt = f"""Du er en SQL-ekspert. Oversett følgende spørsmål til en DuckDB SQL-spørring.
{schema_ctx}

Spørsmål: {question}

Regler:
- Bruk kun DuckDB-kompatibel SQL
- Returner kun SQL-koden, ingen forklaring
- Bruk tabellanavnet fra konteksten hvis oppgitt
- Begrens resultater til 1000 rader med LIMIT 1000
- Kommenter på norsk med -- hvis logikken er kompleks"""

    try:
        import anthropic
        client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        sql = message.content[0].text.strip()
        # Fjern markdown code blocks hvis modellen returnerte dem
        if sql.startswith("```"):
            sql = "\n".join(sql.split("\n")[1:])
        if sql.endswith("```"):
            sql = "\n".join(sql.split("\n")[:-1])
        sql = sql.strip()
        return {"sql": sql}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"NL2SQL feilet: {exc}")


@app.get("/api/products/{product_id}/sla", tags=["products"], summary="SLA-ferskhets-status")
def api_get_sla(product_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    manifest = _resolve_product(product_id)
    user     = auth.get_current_user(request)
    _require_access(manifest, user, api_key)
    result = _safe_sla(product_id)
    if result is None:
        result = _compute_sla_live(manifest)
    return result


@app.get("/api/products/{product_id}/sla/history", tags=["products"], summary="SLA-historikk og MTTR")
def api_get_sla_history(product_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    manifest = _resolve_product(product_id)
    user     = auth.get_current_user(request)
    _require_access(manifest, user, api_key)
    history        = _safe_sla_history(product_id)
    compliance_pct = _sla_compliance_pct(product_id, manifest=manifest)
    mttr           = _mttr(product_id)
    return {
        "product_id":     product_id,
        "history":        history,
        "compliance_pct": compliance_pct,
        "mttr_hours":     mttr,
    }


@app.get("/api/products/{product_id}/lineage", tags=["products"], summary="Datalineage — upstream og downstream")
def api_get_lineage(product_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    """
    Returnerer lineage-graf for produktet:
    - product: grunnleggende manifest-info
    - upstream: rekursivt tre av kildeprodukter (maks 5 nivåer)
    - downstream: produkter som bruker dette produktet som kilde
    - medallion_path: lag produktet finnes i (utledet fra source_path)
    - column_lineage: kolonnenivå mapping om registrert
    - mermaid: Mermaid flowchart-streng for visualisering
    """
    manifest = _resolve_product(product_id)
    user     = auth.get_current_user(request)
    _require_access(manifest, user, api_key)

    idx        = {m["id"]: m for m in list_all()}
    upstream   = _build_upstream(product_id, idx, {product_id})
    downstream = _find_downstream(product_id, idx)
    layer      = _source_layer(manifest.get("source_path", ""))

    return {
        "product": {
            "id":           manifest["id"],
            "name":         manifest.get("name", product_id),
            "product_type": manifest.get("product_type", "source"),
            "domain":       manifest.get("domain"),
            "owner":        manifest.get("owner"),
        },
        "upstream":       upstream,
        "downstream":     downstream,
        "medallion_path": [layer] if layer else [],
        "column_lineage": manifest.get("column_lineage") or [],
        "mermaid":        _mermaid_lineage(manifest, upstream, downstream),
    }


@app.get("/api/products/{product_id}/versions", tags=["products"], summary="Versjonshistorikk med diff")
def api_get_versions(product_id: str):
    _resolve_product(product_id)
    history = list_versions(product_id)
    if not history:
        return []
    result = []
    for i, entry in enumerate(history):
        prev_manifest = history[i + 1]["manifest"] if i + 1 < len(history) else {}
        changes = _diff_manifests(prev_manifest, entry["manifest"])
        result.append({
            "version":       entry["version"],
            "registered_at": entry["registered_at"],
            "changes":       changes,
            "manifest":      entry["manifest"],
        })
    return result


@app.get("/api/products/{product_id}", tags=["products"], summary="Hent ett produkt med historikk")
def api_get_product(product_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    manifest = _resolve_product(product_id)
    user     = auth.get_current_user(request)
    _require_access(manifest, user, api_key)
    pii_ok   = _has_pii_access(user) or api_key == PORTAL_API_KEY
    m        = dict(manifest)
    if m.get("schema"):
        m["schema"] = _mask_schema(m["schema"], pii_ok)
    return {"manifest": m, "history": list_versions(product_id)}


# ── API: schema-metadata ───────────────────────────────────────────────────────

@app.patch("/api/products/{product_id}/schema", tags=["products"], summary="Oppdater kolonnemetadata (PII, sensitivitet, beskrivelse)")
async def api_patch_schema(product_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    """
    Oppdater PII-merking, sensitivitetsnivå og beskrivelse for enkeltkolonner.

    Krever at innlogget bruker er eier av produktet eller admin.
    Registrerer en ny manifestversjon (patch-bump) uten å endre selve dataene.

    Body: {"columns": [{"name": "col", "pii": true, "sensitivity": "high", "description": "..."}]}
    """
    user = auth.get_current_user(request)
    if not user and api_key != PORTAL_API_KEY:
        raise HTTPException(status_code=401, detail="Krever innlogging eller API-nøkkel")

    manifest = _resolve_product(product_id)
    is_owner = user and (user["role"] == "admin" or manifest.get("owner") == user.get("username"))
    if not is_owner and api_key != PORTAL_API_KEY:
        raise HTTPException(status_code=403, detail="Kun produkteier eller admin kan oppdatere schema-metadata")

    body    = await request.json()
    updates = {col["name"]: col for col in body.get("columns", []) if col.get("name")}
    if not updates:
        raise HTTPException(status_code=422, detail="columns er påkrevd og kan ikke være tom")

    # Hent eksisterende schema fra manifest (kan mangle for eldre produkter)
    existing_schema = manifest.get("schema") or []
    existing_by_name = {col["name"]: col for col in existing_schema}

    # Hent Delta-schema for å kjenne alle kolonnenavn (bruk cache hvis tilgjengelig)
    delta_schema = manifest.get("delta_schema") or _safe_schema(manifest["source_path"])
    all_col_names = (
        [f["name"] for f in delta_schema]
        if delta_schema
        else [c["name"] for c in existing_schema]
    )

    # Valider at kolonnenavn som oppdateres faktisk finnes
    unknown = [n for n in updates if n not in all_col_names]
    if unknown:
        raise HTTPException(status_code=422, detail=f"Ukjente kolonner: {unknown}")

    # Bygg oppdatert schema — bevar eksisterende metadata, merge inn oppdateringer
    new_schema = []
    for col_name in all_col_names:
        base = dict(existing_by_name.get(col_name, {"name": col_name}))
        if col_name in updates:
            upd = updates[col_name]
            # Tillat eksplisitt fjerning (pii=false fjerner feltet)
            if "pii" in upd:
                if upd["pii"]:
                    base["pii"] = True
                else:
                    base.pop("pii", None)
            if "sensitivity" in upd:
                if upd["sensitivity"]:
                    base["sensitivity"] = upd["sensitivity"]
                else:
                    base.pop("sensitivity", None)
            if "description" in upd:
                if upd["description"]:
                    base["description"] = upd["description"]
                else:
                    base.pop("description", None)
        new_schema.append(base)

    # Bump patch-versjon og registrer ny manifestversjon
    updated_manifest = {
        **manifest,
        "schema":  new_schema,
        "version": _bump_patch(manifest["version"]),
    }
    register(updated_manifest)
    return {"status": "updated", "product_id": product_id, "version": updated_manifest["version"]}


# ── API: tilgangsforespørsler ──────────────────────────────────────────────────

@app.post("/api/access-requests", tags=["access"], summary="Be om tilgang til restricted produkt")
def api_create_access_request(body: dict, request: Request):
    user = auth.get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Logg inn for å be om tilgang")
    product_id = body.get("product_id")
    if not product_id:
        raise HTTPException(status_code=422, detail="product_id er påkrevd")
    _resolve_product(product_id)  # verifiser at produktet finnes
    result = auth.create_access_request(user["id"], product_id)
    return result


@app.get("/api/access-requests", tags=["access"], summary="Liste tilgangsforespørsler (admin)")
def api_list_access_requests(request: Request, status: str | None = None):
    user = auth.get_current_user(request)
    _require_admin_api(user)
    return auth.list_access_requests(status)


@app.post("/api/access-requests/{request_id}/resolve", tags=["access"], summary="Godkjenn/avslå forespørsel")
def api_resolve_access_request(request_id: str, body: dict, request: Request):
    user = auth.get_current_user(request)
    _require_admin_api(user)
    approved = body.get("approved", False)
    result   = auth.resolve_access_request(request_id, approved, user["username"])
    if not result:
        raise HTTPException(status_code=404, detail="Forespørsel ikke funnet eller allerede behandlet")
    return result


# ── API: admin ─────────────────────────────────────────────────────────────────

@app.get("/api/admin/users", tags=["admin"], summary="Liste brukere (admin)")
def api_list_users(request: Request):
    user = auth.get_current_user(request)
    _require_admin_api(user)
    return auth.list_users()


@app.patch("/api/admin/users/{user_id}/role", tags=["admin"], summary="Endre brukerrolle (admin)")
def api_update_role(user_id: str, body: dict, request: Request):
    user = auth.get_current_user(request)
    _require_admin_api(user)
    role = body.get("role")
    if role not in ("admin", "user"):
        raise HTTPException(status_code=422, detail="role må være 'admin' eller 'user'")
    if user_id == user["id"]:
        raise HTTPException(status_code=400, detail="Kan ikke endre egen rolle")
    auth.update_user_role(user_id, role)
    return {"status": "updated"}


@app.delete("/api/admin/users/{user_id}", tags=["admin"], summary="Slett bruker (admin)")
def api_delete_user(user_id: str, request: Request):
    user = auth.get_current_user(request)
    _require_admin_api(user)
    if user_id == user["id"]:
        raise HTTPException(status_code=400, detail="Kan ikke slette seg selv")
    auth.delete_user(user_id)
    return {"status": "deleted"}


# ── API: governance / PII-kart ─────────────────────────────────────────────────

@app.get("/api/governance/pii-map", tags=["governance"], summary="GDPR-datakart over PII-kolonner")
def api_pii_map(request: Request, api_key: str | None = Security(_api_key_scheme)):
    """Returnerer alle produkter med PII-kolonner for GDPR-dokumentasjon."""
    user = auth.get_current_user(request)
    if not user and api_key != PORTAL_API_KEY:
        raise HTTPException(status_code=401, detail="Krever innlogging eller API-nøkkel")
    result = []
    for m in list_all():
        pii_cols = [
            {
                "column":      col["name"],
                "type":        col.get("type"),
                "sensitivity": col.get("sensitivity", "unknown"),
                "nullable":    col.get("nullable"),
            }
            for col in (m.get("schema") or [])
            if col.get("pii")
        ]
        # Include products that either have pii_columns field or have pii=true columns in schema
        pii_columns_field = m.get("pii_columns", [])
        if pii_cols or pii_columns_field:
            result.append({
                "product_id":    m["id"],
                "name":          m.get("name", m["id"]),
                "domain":        m.get("domain"),
                "owner":         m.get("owner"),
                "source_path":   m.get("source_path"),
                "pii_columns":   pii_cols,
                "sensitivity":   max((c.get("sensitivity", "low") for c in pii_cols), default="unknown",
                                     key=lambda s: {"low": 0, "medium": 1, "high": 2}.get(s, -1)),
                "retention_days": (m.get("retention") or {}).get("days"),
                "retention_strategy": (m.get("retention") or {}).get("strategy"),
            })
    return result


@app.post("/api/admin/users/{user_id}/domains", tags=["admin"], summary="Legg til domeneprivilegium")
def api_add_domain(user_id: str, body: dict, request: Request):
    user = auth.get_current_user(request)
    _require_admin_api(user)
    domain = body.get("domain", "").strip()
    if not domain:
        raise HTTPException(status_code=422, detail="domain er påkrevd")
    auth.add_domain_membership(user_id, domain)
    return {"status": "added", "user_id": user_id, "domain": domain}


@app.delete("/api/admin/users/{user_id}/domains/{domain}", tags=["admin"], summary="Fjern domeneprivilegium")
def api_remove_domain(user_id: str, domain: str, request: Request):
    user = auth.get_current_user(request)
    _require_admin_api(user)
    auth.remove_domain_membership(user_id, domain)
    return {"status": "removed", "user_id": user_id, "domain": domain}


@app.get("/api/admin/users/{user_id}/domains", tags=["admin"], summary="Liste domeneprivilegier")
def api_get_user_domains(user_id: str, request: Request):
    user = auth.get_current_user(request)
    _require_admin_api(user)
    return {"user_id": user_id, "domains": auth.get_user_domains(user_id)}


# ── UI: HTML-sider ─────────────────────────────────────────────────────────────

def _check_service(url: str, timeout: float = 2.0) -> bool:
    """Returner True hvis tjenesten svarer på `url` med HTTP < 500."""
    try:
        resp = _HTTPX_CLIENT.get(url, timeout=timeout, follow_redirects=True)
        return resp.status_code < 500
    except Exception:
        return False


def _superset_sqllab_url(manifest: dict, schema: list[dict] | None = None) -> str:
    """Bygg Superset SQL Lab URL med forhåndsutfylt spørring for dette dataproduktet."""
    source_path = manifest.get("source_path", "")
    if not source_path:
        return f"{SUPERSET_EXTERNAL_URL}/sqllab/"
    cols = schema or manifest.get("schema") or []
    if cols:
        col_names = [c["name"] for c in cols if c.get("name")]
        select_clause = "  " + ",\n  ".join(col_names)
    else:
        select_clause = "  *"
    sql = f"SELECT\n{select_clause}\nFROM delta_scan('{source_path}')\nLIMIT 100;"
    return f"{SUPERSET_EXTERNAL_URL}/sqllab/?sql={urllib.parse.quote(sql)}&dbId={SUPERSET_DB_ID}"


def _template_ctx(request: Request, **kwargs) -> dict:
    """Felles malkontekst med innlogget bruker og globale URL-er."""
    return {
        "request":      request,
        "current_user": auth.get_current_user(request),
        "airflow_url":  AIRFLOW_EXTERNAL_URL,
        "jupyter_url":  JUPYTER_EXTERNAL_URL,
        "superset_url": SUPERSET_EXTERNAL_URL,
        "minio_url":    MINIO_EXTERNAL_URL,
        **kwargs,
    }


@app.get("/login", response_class=HTMLResponse, include_in_schema=False)
def page_login(request: Request):
    if auth.get_current_user(request):
        return RedirectResponse("/")
    return templates.TemplateResponse("login.html", _template_ctx(request))


@app.post("/login", response_class=HTMLResponse, include_in_schema=False)
async def page_login_post(
    request:  Request,
    username: str = Form(...),
    password: str = Form(...),
):
    user = auth.get_user_by_username(username)
    if not user or not auth.verify_password(password, user["password_hash"]):
        return templates.TemplateResponse(
            "login.html",
            _template_ctx(request, error="Feil brukernavn eller passord"),
            status_code=401,
        )
    access_token  = auth.create_access_token(user["id"], user["username"], user["role"])
    refresh_token = auth.create_refresh_token(user["id"])
    response      = RedirectResponse("/", status_code=303)
    _set_auth_cookies(response, access_token, refresh_token)
    return response


@app.get("/logout", include_in_schema=False)
def page_logout(request: Request):
    refresh_token = request.cookies.get("refresh_token")
    if refresh_token:
        auth.revoke_refresh_token(refresh_token)
    response = RedirectResponse("/login", status_code=303)
    _clear_auth_cookies(response)
    return response


@app.get("/register", response_class=HTMLResponse, include_in_schema=False)
def page_register(request: Request):
    if auth.get_current_user(request):
        return RedirectResponse("/")
    return templates.TemplateResponse("register.html", _template_ctx(request))


@app.post("/register", response_class=HTMLResponse, include_in_schema=False)
async def page_register_post(
    request:  Request,
    username: str = Form(...),
    email:    str = Form(...),
    password: str = Form(...),
):
    if len(password) < 6:
        return templates.TemplateResponse(
            "register.html",
            _template_ctx(request, error="Passord må være minst 6 tegn"),
            status_code=422,
        )
    try:
        user = auth.create_user(username.strip(), email.strip(), password)
    except Exception as exc:
        msg = "Brukernavn eller e-post er allerede registrert" if "UNIQUE" in str(exc) else str(exc)
        return templates.TemplateResponse(
            "register.html",
            _template_ctx(request, error=msg),
            status_code=409,
        )
    access_token  = auth.create_access_token(user["id"], user["username"], user["role"])
    refresh_token = auth.create_refresh_token(user["id"])
    response      = RedirectResponse("/", status_code=303)
    _set_auth_cookies(response, access_token, refresh_token)
    return response


@app.get("/admin", response_class=HTMLResponse, include_in_schema=False)
def page_admin(request: Request):
    user = auth.get_current_user(request)
    if not user:
        return RedirectResponse("/login?next=/admin", status_code=303)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Krever admin-rolle")
    users    = auth.list_users()
    requests = auth.list_access_requests()
    pending  = [r for r in requests if r["status"] == "pending"]
    # Berik brukere med domener
    for u in users:
        u["domains"] = auth.get_user_domains(u["id"])
    domains      = sorted({m.get("domain") for m in list_all() if m.get("domain")})
    top_products = auth.get_usage_counts(days=30)
    return templates.TemplateResponse(
        "admin.html",
        _template_ctx(request, users=users, access_requests=requests,
                      pending_count=len(pending), all_domains=domains,
                      top_products=top_products),
    )


@app.post("/admin/access-requests/{request_id}", include_in_schema=False)
async def page_resolve_request(request_id: str, request: Request):
    user = auth.get_current_user(request)
    if not user or user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Krever admin-rolle")
    form     = await request.form()
    approved = form.get("action") == "approve"
    auth.resolve_access_request(request_id, approved, user["username"])
    return RedirectResponse("/admin", status_code=303)


@app.post("/access-requests", include_in_schema=False)
async def page_create_access_request(request: Request):
    user = auth.get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    form       = await request.form()
    product_id = form.get("product_id")
    if product_id:
        auth.create_access_request(user["id"], product_id)
    return RedirectResponse(f"/products/{product_id}", status_code=303)


@app.get("/browse", response_class=HTMLResponse, include_in_schema=False)
def page_browse_redirect(request: Request):
    user = auth.get_current_user(request)
    if not user:
        return RedirectResponse("/login?next=/browse", status_code=303)
    return templates.TemplateResponse("browse.html", _template_ctx(request))


@app.post("/api/create-analytical", tags=["jupyter"], summary="Opprett analytisk dataprodukt")
async def api_create_analytical(request: Request):
    """
    Genererer notebook og (valgfritt) Airflow DAG for et nytt analytisk dataprodukt.
    Body: {product_id, name, description, domain, owner, source_ids, tags,
           freshness_hours, access, schedule, dag_id}
    """
    user = auth.get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Logg inn for å opprette analytiske produkter")

    body            = await request.json()
    product_id      = body.get("product_id", "").strip()
    name            = body.get("name", "").strip()
    description     = body.get("description", "").strip()
    domain          = body.get("domain", "analytics").strip()
    owner           = body.get("owner", user["username"]).strip()
    source_ids      = body.get("source_ids", [])
    tags            = body.get("tags", [])
    freshness_hours = body.get("freshness_hours")
    access          = body.get("access", "public")
    schedule        = body.get("schedule", "manual")
    dag_id          = body.get("dag_id", "").strip() or re.sub(r"[^a-z0-9_]", "_", product_id)

    if not product_id or not name:
        raise HTTPException(status_code=422, detail="product_id og name er påkrevd")

    # Hent manifester for kildeprodukter
    manifests = []
    for sid in source_ids:
        try:
            manifests.append(get(sid))
        except KeyError:
            raise HTTPException(status_code=404, detail=f"Kildeprodukt '{sid}' ikke funnet")

    # Generer notebook
    nb           = _generate_analytical_notebook(
        manifests, product_id, name, description, domain, owner,
        source_ids, tags, freshness_hours, access,
    )
    nb_filename  = _safe_filename(product_id)
    nb_path      = _notebook_path(nb_filename)
    pathlib.Path(NOTEBOOKS_DIR).mkdir(parents=True, exist_ok=True)
    # Opprett output/-mappe for Papermill
    pathlib.Path(NOTEBOOKS_DIR, "output").mkdir(parents=True, exist_ok=True)
    nb_path.write_text(json.dumps(nb, ensure_ascii=False, indent=1))

    result = {
        "notebook_filename": nb_filename,
        "notebook_url":      _jupyter_open_url(nb_filename),
        "dag_created":       False,
    }

    # Generer Airflow DAG om schedule er valgt
    if schedule != "manual":
        dag_content  = _generate_airflow_dag(dag_id, product_id, name, nb_filename, schedule, domain, owner)
        dag_filename = f"{dag_id}.py"
        dag_path     = pathlib.Path(DAGS_DIR) / dag_filename
        try:
            dag_path.write_text(dag_content)
            result["dag_created"]  = True
            result["dag_id"]       = dag_id
            result["dag_filename"] = dag_filename
            result["schedule"]     = schedule
        except Exception as exc:
            result["dag_warning"] = f"Notebook OK, men DAG-fil kunne ikke skrives: {exc}"

    return result


@app.get("/create-analytical", response_class=HTMLResponse, include_in_schema=False)
def page_create_analytical(request: Request):
    user = auth.get_current_user(request)
    if not user:
        return RedirectResponse("/login?next=/create-analytical", status_code=303)
    all_products = list_all()
    return templates.TemplateResponse("create_analytical.html", _template_ctx(
        request,
        all_products=all_products,
        schedule_labels=_SCHEDULE_LABELS,
    ))


@app.get("/publish", response_class=HTMLResponse, include_in_schema=False)
def page_publish(request: Request, path: str = ""):
    user = auth.get_current_user(request)
    if not user:
        return RedirectResponse("/login?next=/publish", status_code=303)
    schema    = _safe_schema(path) if path else None
    col_count = len(schema) if schema else 0
    return templates.TemplateResponse("publish.html", _template_ctx(
        request,
        source_path=path,
        schema=schema or [],
        col_count=col_count,
    ))


@app.post("/publish", response_class=HTMLResponse, include_in_schema=False)
async def page_publish_post(request: Request):
    user = auth.get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)

    form = await request.form()

    def fv(key: str) -> str:
        return (form.get(key) or "").strip()

    product_id  = fv("id")
    name        = fv("name")
    domain      = fv("domain")
    owner       = fv("owner")
    description = fv("description")
    source_path = fv("source_path")
    version     = fv("version") or "1.0"
    tags_raw    = fv("tags")
    freshness   = fv("freshness_hours")
    access      = fv("access") or "public"

    errors: list[str] = []
    if not product_id:
        errors.append("Produkt-ID er påkrevd")
    if not name:
        errors.append("Navn er påkrevd")
    if not domain:
        errors.append("Domene er påkrevd")
    if not owner:
        errors.append("Eier er påkrevd")
    if not source_path:
        errors.append("Kildesti er påkrevd")

    schema    = _safe_schema(source_path) if source_path else []
    col_count = len(schema) if schema else 0

    if errors:
        return templates.TemplateResponse(
            "publish.html",
            _template_ctx(
                request,
                source_path=source_path,
                schema=schema,
                col_count=col_count,
                errors=errors,
                form_data=dict(form),
            ),
            status_code=422,
        )

    tags = [t.strip() for t in tags_raw.split(",") if t.strip()]

    manifest: dict = {
        "id":          product_id,
        "name":        name,
        "domain":      domain,
        "owner":       owner,
        "description": description,
        "version":     version,
        "source_path": source_path,
        "format":      "delta",
        "access":      access,
        "tags":        tags,
        "schema":      schema,
    }
    if freshness:
        try:
            manifest["quality_sla"] = {"freshness_hours": int(freshness)}
        except ValueError:
            pass

    try:
        register(manifest)
    except Exception as exc:
        return templates.TemplateResponse(
            "publish.html",
            _template_ctx(
                request,
                source_path=source_path,
                schema=schema,
                col_count=col_count,
                errors=[f"Registrering feilet: {exc}"],
                form_data=dict(form),
            ),
            status_code=500,
        )

    return RedirectResponse(f"/products/{product_id}", status_code=303)


@app.get("/health", include_in_schema=False)
def health_check():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def page_catalog(request: Request):
    user     = auth.get_current_user(request)
    products = [
        p for p in list_all()
        if _check_product_access(p, user, None)
    ]
    for p in products:
        p["_quality"]  = _safe_quality(p["id"])
        p["_pipeline"] = _safe_pipeline(p.get("dag_id"))
        p["_sla"]      = _safe_sla(p["id"])
    domains  = sorted({p["domain"] for p in products})
    all_tags = sorted({tag for p in products for tag in p.get("tags", [])})
    anomaly_map = {}
    for m in products:
        a = _safe_anomalies(m["id"])
        if a and a.get("has_anomaly"):
            anomaly_map[m["id"]] = True
    view_counts = {r["product_id"]: r["views"] for r in auth.get_usage_counts(30)}
    return templates.TemplateResponse("catalog.html", _template_ctx(
        request,
        products=products,
        domains=domains,
        all_tags=all_tags,
        anomaly_map=anomaly_map,
        view_counts=view_counts,
    ))


@app.get("/products/{product_id}", response_class=HTMLResponse, include_in_schema=False)
def page_product(request: Request, product_id: str):
    try:
        manifest = get(product_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Produkt '{product_id}' ikke funnet")
    user = auth.get_current_user(request)

    # Vis låst side for restricted produkter brukeren ikke har tilgang til
    is_restricted = manifest.get("access") == "restricted"
    has_access    = _check_product_access(manifest, user, None)

    with timer("registry.list_versions"):
        raw_history = list_versions(product_id)
    versioned_history = []
    for i, entry in enumerate(raw_history):
        prev = raw_history[i + 1]["manifest"] if i + 1 < len(raw_history) else {}
        versioned_history.append({
            **entry,
            "changes": _diff_manifests(prev, entry["manifest"]),
        })

    # Bruk cachet delta_schema fra pipeline_stats; les Delta-log kun som fallback
    cached_delta_schema = manifest.get("delta_schema")
    delta_schema = (
        cached_delta_schema
        if (has_access and cached_delta_schema)
        else (_safe_schema(manifest["source_path"]) if has_access else None)
    )
    schema       = _merge_schema(delta_schema, manifest.get("schema"))
    quality      = _safe_quality(product_id) if has_access else None
    anomalies = _safe_anomalies(product_id) if has_access else None
    incidents = auth.list_incidents(product_id=product_id) if has_access else []
    quality_history = _safe_quality_history(product_id) if has_access else []
    pipeline   = _safe_pipeline(manifest.get("dag_id"))
    idp_status = _safe_idp_status(manifest) if has_access else None
    sla        = (_safe_sla(product_id) or _compute_sla_live(manifest)) if has_access else None
    sla_compliance_pct = _sla_compliance_pct(product_id, manifest=manifest) if has_access else None
    mttr_hours         = _mttr(product_id) if has_access else None

    # Sjekk om brukeren allerede har en pending forespørsel
    pending_request = False
    if user and is_restricted and not has_access:
        existing = auth.list_access_requests()
        pending_request = any(
            r["status"] == "pending"
            and r["username"] == user["username"]
            and r["product_id"] == product_id
            for r in existing
        )

    # Løs opp source_products til manifester for "Basert på"-seksjon
    source_product_manifests = []
    for spid in manifest.get("source_products", []):
        try:
            source_product_manifests.append(get(spid))
        except KeyError:
            source_product_manifests.append({"id": spid, "name": spid})

    # Lineage (#62/#63)
    with timer("registry.list_all"):
        _all_products = list_all()
    _idx        = {m["id"]: m for m in _all_products}
    with timer("compute.lineage"):
        upstream    = _build_upstream(product_id, _idx, {product_id})
        downstream  = _find_downstream(product_id, _idx)
    mermaid_lineage = _mermaid_lineage(manifest, upstream, downstream)
    column_lineage  = manifest.get("column_lineage") or []

    related = _related_products(product_id, manifest, _all_products) if has_access else []
    views   = auth.get_product_views(product_id, days=30)

    # Sjekk om notebook allerede finnes
    nb_filename     = _safe_filename(product_id)
    nb_exists       = _notebook_path(nb_filename).exists()
    jupyter_nb_url  = _jupyter_open_url(nb_filename) if nb_exists else None

    subscribers   = auth.list_subscribers(product_id) if user and (user["role"] == "admin" or manifest.get("owner") == user.get("username")) else None
    is_subscribed = auth.is_subscribed(user["id"], product_id) if user else False
    is_owner      = bool(user and (user["role"] == "admin" or manifest.get("owner") == user.get("username")))

    return templates.TemplateResponse("product.html", _template_ctx(
        request,
        manifest=manifest,
        history=versioned_history,
        schema=schema,
        superset_sqllab_url=_superset_sqllab_url(manifest, schema),
        quality=quality,
        pipeline=pipeline,
        idp_status=idp_status,
        sla=sla,
        airflow_url=AIRFLOW_EXTERNAL_URL,
        has_access=has_access,
        is_restricted=is_restricted,
        pending_request=pending_request,
        nb_filename=nb_filename,
        jupyter_nb_url=jupyter_nb_url,
        source_product_manifests=source_product_manifests,
        upstream=upstream,
        downstream=downstream,
        mermaid_lineage=mermaid_lineage,
        column_lineage=column_lineage,
        subscribers=subscribers,
        is_subscribed=is_subscribed,
        is_owner=is_owner,
        anomalies=anomalies,
        incidents=incidents,
        quality_history=quality_history,
        related=related,
        views=views,
        sla_compliance_pct=sla_compliance_pct,
        mttr_hours=mttr_hours,
    ))


@timed("airflow.list_dags")
def _list_airflow_dags() -> list[str]:
    """Hent alle DAG-IDer fra Airflow. Returnerer tom liste ved feil."""
    try:
        resp = _HTTPX_CLIENT.get(
            f"{AIRFLOW_URL}/api/v1/dags",
            params={"limit": 100, "only_active": "true"},
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            timeout=5.0,
        )
        resp.raise_for_status()
        return [d["dag_id"] for d in resp.json().get("dags", [])]
    except Exception:
        return []


@app.get("/pipelines", response_class=HTMLResponse, include_in_schema=False)
def page_pipelines(request: Request):
    products = list_all()

    # Bygg oppslag: dag_id → liste av produktnavn (fra manifester som har dag_id satt)
    dag_products: dict[str, list[str]] = {}
    for p in products:
        if p.get("dag_id"):
            dag_products.setdefault(p["dag_id"], []).append(p["name"])

    # Hent alle aktive DAGs fra Airflow — vis dem uansett om de er koblet til et produkt
    airflow_dag_ids = _list_airflow_dags()

    # Slå sammen: alle kjente DAG-IDer (fra Airflow + fra manifester)
    all_dag_ids = list(dict.fromkeys(airflow_dag_ids + list(dag_products.keys())))

    dags = []
    for dag_id in all_dag_ids:
        last_run = _safe_pipeline(dag_id)
        timeline = _get_dag_timeline(dag_id)
        dags.append({
            "dag_id":   dag_id,
            "products": dag_products.get(dag_id, []),
            "last_run": last_run,
            "timeline": timeline,
        })

    return templates.TemplateResponse("pipelines.html", _template_ctx(
        request,
        dags=dags,
        airflow_ui=AIRFLOW_EXTERNAL_URL,
    ))


@app.get("/platform", response_class=HTMLResponse, include_in_schema=False)
def page_platform(request: Request):
    """Plattformoversikt — status og lenker for alle tjenester."""
    services = [
        {
            "name":       "Apache Airflow",
            "desc":       "Pipeline-orkestrering, DAG-planlegging og kjøringsovervåking",
            "icon":       "bi-diagram-3",
            "color":      "#017cee",
            "bg":         "#e8f3ff",
            "ext_url":    AIRFLOW_EXTERNAL_URL,
            "health_url": f"{AIRFLOW_URL}/health",
            "port":       "8080",
        },
        {
            "name":       "Jupyter Lab",
            "desc":       "Interaktiv notebook for dataanalyse, eksperimentering og publisering",
            "icon":       "bi-journal-code",
            "color":      "#F37626",
            "bg":         "#fff3eb",
            "ext_url":    JUPYTER_EXTERNAL_URL,
            "health_url": f"{JUPYTER_URL}/api",
            "port":       "8888",
        },
        {
            "name":       "Apache Superset",
            "desc":       "BI-dashboards og interaktiv SQL-utforskning mot Delta Lake",
            "icon":       "bi-bar-chart-line",
            "color":      "#20A7C9",
            "bg":         "#e6f6fb",
            "ext_url":    SUPERSET_EXTERNAL_URL,
            "health_url": f"{SUPERSET_URL}/health",
            "port":       "8088",
        },
        {
            "name":       "MinIO Console",
            "desc":       "S3-kompatibel objektlagring — raw, bronze, silver, gold, analytics",
            "icon":       "bi-bucket-fill",
            "color":      "#C72C48",
            "bg":         "#fdeaed",
            "ext_url":    MINIO_EXTERNAL_URL,
            "health_url": f"{MINIO_ENDPOINT.replace(':9000', ':9001')}/minio/health/live",
            "port":       "9001",
        },
    ]

    for svc in services:
        svc["online"] = _check_service(svc["health_url"])

    products  = list_all()
    medallion = [
        {"layer": "raw",       "icon": "bi-cloud-upload",    "desc": "Rådata fra kildesystemer"},
        {"layer": "bronze",    "icon": "bi-database",        "desc": "Innlest til Delta Lake"},
        {"layer": "silver",    "icon": "bi-funnel",          "desc": "Renset og validert"},
        {"layer": "gold",      "icon": "bi-star",            "desc": "Domeneprodukter"},
        {"layer": "analytics", "icon": "bi-graph-up-arrow",  "desc": "Analytiske produkter"},
    ]

    return templates.TemplateResponse("platform.html", _template_ctx(
        request,
        services=services,
        product_count=len(products),
        medallion=medallion,
    ))


@app.get("/governance", response_class=HTMLResponse, include_in_schema=False)
def page_governance(request: Request):
    user = auth.get_current_user(request)
    if not user:
        return RedirectResponse("/login?next=/governance", status_code=303)
    # Bygg PII-kart
    pii_map = []
    for m in list_all():
        pii_cols = [
            col for col in (m.get("schema") or []) if col.get("pii")
        ]
        if pii_cols:
            pii_map.append({
                "product_id":    m["id"],
                "name":          m.get("name", m["id"]),
                "domain":        m.get("domain"),
                "owner":         m.get("owner"),
                "source_path":   m.get("source_path"),
                "pii_columns":   pii_cols,
                "retention":     m.get("retention"),
            })
    domains = sorted({m.get("domain") for m in list_all() if m.get("domain")})
    return templates.TemplateResponse(
        "governance.html",
        _template_ctx(request, pii_map=pii_map, domains=domains, pii_access=_has_pii_access(user)),
    )


@app.get("/observability", response_class=HTMLResponse, include_in_schema=False)
def page_observability(request: Request):
    user = auth.get_current_user(request)
    if not user:
        return RedirectResponse("/login?next=/observability", status_code=303)

    products     = list_all()
    all_domains  = sorted({m.get("domain") for m in products if m.get("domain")})
    product_data = []

    for m in products:
        pid      = m["id"]
        quality  = _safe_quality(pid)
        anomaly  = _safe_anomalies(pid)
        incidents = auth.list_incidents(product_id=pid)
        open_incidents = [i for i in incidents if i["status"] != "resolved"]

        product_data.append({
            "id":             pid,
            "name":           m.get("name", pid),
            "domain":         m.get("domain"),
            "owner":          m.get("owner"),
            "quality":        quality,
            "anomaly":        anomaly,
            "open_incidents": len(open_incidents),
            "incidents":      incidents[:5],
        })

    # Beregn plattformhelse som vektet gjennomsnitt av score_pct
    scored = [p for p in product_data if p["quality"] and p["quality"].get("score_pct") is not None]
    platform_health = round(sum(p["quality"]["score_pct"] for p in scored) / len(scored), 1) if scored else None

    return templates.TemplateResponse(
        "observability.html",
        _template_ctx(
            request,
            product_data=product_data,
            all_domains=all_domains,
            platform_health=platform_health,
        ),
    )


@app.get("/pipeline-builder", response_class=HTMLResponse, include_in_schema=False)
def page_pipeline_builder(request: Request):
    """Selvbetjent pipeline-builder — velg mal, konfigurer og deploy."""
    user = auth.get_current_user(request)
    if not user:
        return RedirectResponse("/login?next=/pipeline-builder", status_code=303)
    return templates.TemplateResponse("pipeline_builder.html", _template_ctx(
        request,
        templates=_PIPELINE_TEMPLATES,
    ))


@app.get("/pipelines/{dag_id}/edit", response_class=HTMLResponse, include_in_schema=False)
def page_pipeline_edit(request: Request, dag_id: str):
    """Rediger eksisterende pipeline-konfigurasjon."""
    user = auth.get_current_user(request)
    if not user:
        return RedirectResponse(f"/login?next=/pipelines/{dag_id}/edit", status_code=303)
    stored   = _load_pipeline_config(dag_id)
    history  = _load_pipeline_history(dag_id)
    last_run = _safe_pipeline(dag_id)
    hint     = _diagnose_error(last_run.get("error")) if last_run else None
    return templates.TemplateResponse("pipeline_edit.html", _template_ctx(
        request,
        dag_id=dag_id,
        stored=stored,
        history=history,
        templates=_PIPELINE_TEMPLATES,
        last_run=last_run,
        hint=hint,
    ))


def _contract_status(manifest: dict, sla_result: dict | None) -> str:
    """
    Returner trafikklys-status for kontrakt: 'ok', 'warning', 'breach', 'none'.
    """
    contract = manifest.get("contract")
    if not contract:
        return "none"
    slo = contract.get("slo") or {}
    if not sla_result:
        return "none"
    compliant = sla_result.get("compliant")
    if compliant is None:
        return "none"
    if not compliant:
        return "breach"
    freshness_hours  = slo.get("freshness_hours")
    hours_since      = sla_result.get("hours_since_update")
    if freshness_hours and hours_since is not None:
        ratio = hours_since / freshness_hours
        if ratio > 0.8:
            return "warning"
    return "ok"


@app.get("/domain/{domain}/contracts", response_class=HTMLResponse, include_in_schema=False)
def page_domain_contracts(request: Request, domain: str):
    """Kontrakts-dashboard for et domene — trafikklys per produkt."""
    all_products = [p for p in list_all() if p.get("domain") == domain]
    if not all_products and domain:
        # Sjekk om domenet eksisterer i det hele tatt
        known_domains = {p.get("domain") for p in list_all()}
        if domain not in known_domains:
            raise HTTPException(status_code=404, detail=f"Domene '{domain}' ikke funnet")

    rows = []
    for p in all_products:
        sla    = _safe_sla(p["id"]) or _compute_sla_live(p)
        status = _contract_status(p, sla)
        rows.append({
            "manifest": p,
            "sla":      sla,
            "status":   status,
        })

    # Sorter: brudd øverst, deretter advarsel, OK, ingen kontrakt
    _order = {"breach": 0, "warning": 1, "ok": 2, "none": 3}
    rows.sort(key=lambda r: _order.get(r["status"], 4))

    return templates.TemplateResponse("contracts.html", _template_ctx(
        request,
        domain=domain,
        rows=rows,
        all_domains=sorted({p.get("domain", "") for p in list_all()}),
    ))


@app.get("/domain/{domain}/contracts.csv", include_in_schema=False)
def export_domain_contracts_csv(request: Request, domain: str):
    """Eksporter kontrakt-status for domenet som CSV."""
    import csv, io
    all_products = [p for p in list_all() if p.get("domain") == domain]
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["product_id", "name", "owner", "version", "schema_compatibility",
                     "freshness_hours", "completeness_pct", "sla_status", "contract_status"])
    for p in all_products:
        sla      = _safe_sla(p["id"]) or _compute_sla_live(p)
        status   = _contract_status(p, sla)
        contract = p.get("contract") or {}
        slo      = contract.get("slo") or {}
        writer.writerow([
            p["id"], p.get("name"), p.get("owner"), p.get("version"),
            contract.get("schema_compatibility", "NONE"),
            slo.get("freshness_hours", ""),
            slo.get("completeness_pct", ""),
            "compliant" if (sla or {}).get("compliant") else "breach",
            status,
        ])
    return JSONResponse(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=contracts_{domain}.csv"},
    )
