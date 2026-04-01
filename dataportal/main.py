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
  GET  /api/products/{id}/versions     — versjonshistorikk med diff
  GET  /api/browser                    — naviger MinIO og detekter Delta-tabeller

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
"""

import json
import os
import pathlib
import re
import sys
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
from registry import get, list_all, list_versions, register  # noqa: E402

import auth  # noqa: E402

# ── oppstart ───────────────────────────────────────────────────────────────────

auth.init_db()

# ── konfigurasjon ──────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",  "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "changeme")

AIRFLOW_URL  = os.environ.get("AIRFLOW_URL",  "http://airflow-webserver:8080")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.environ.get("AIRFLOW_PASS", "admin")

PORTAL_API_KEY  = os.environ.get("PORTAL_API_KEY", "dev-key-change-me")
JUPYTER_URL     = os.environ.get("JUPYTER_URL",    "http://localhost:8888")
NOTEBOOKS_DIR   = os.environ.get("NOTEBOOKS_DIR",  "/opt/dataportal/notebooks")
_api_key_scheme = APIKeyHeader(name="X-API-Key", auto_error=False)

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
    version="2.0.0",
    description="REST API for dataprodukter, pipeline-status og datakvalitet.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

templates = Jinja2Templates(directory="/opt/dataportal/templates")

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


def _get_dag_timeline(dag_id: str, days: int = 7) -> list[dict]:
    today = datetime.now(tz=timezone.utc).date()
    _priority = {"failed": 4, "running": 3, "queued": 2, "success": 1, "none": 0}
    date_status: dict[str, str] = {
        (today - timedelta(days=i)).isoformat(): "none"
        for i in range(days - 1, -1, -1)
    }
    try:
        since = (today - timedelta(days=days)).isoformat() + "T00:00:00Z"
        resp  = httpx.get(
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


def _safe_schema(source_path: str) -> list[dict] | None:
    try:
        dt     = DeltaTable(source_path, storage_options=_STORAGE_OPTIONS)
        fields = json.loads(dt.schema().to_json()).get("fields", [])
        return [{"name": f["name"], "type": str(f["type"]), "nullable": f.get("nullable", True)}
                for f in fields]
    except Exception:
        return None


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
    return f"{JUPYTER_URL}/lab/tree/{filename}"


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

    schema  = _safe_schema(manifest["source_path"])
    quality = _safe_quality(product_id)
    nb      = _generate_product_notebook(manifest, quality, schema)

    pathlib.Path(NOTEBOOKS_DIR).mkdir(parents=True, exist_ok=True)
    nb_path.write_text(json.dumps(nb, ensure_ascii=False, indent=1))

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
    required = {"id", "name", "domain", "owner", "version", "source_path", "format"}
    missing  = required - manifest.keys()
    if missing:
        raise HTTPException(status_code=422, detail=f"Manglende felt: {sorted(missing)}")
    register(manifest)
    return {"status": "registered", "id": manifest["id"]}


@app.get("/api/products/{product_id}/schema", tags=["products"], summary="Delta-tabellens schema")
def api_get_schema(product_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    manifest = _resolve_product(product_id)
    user     = auth.get_current_user(request)
    _require_access(manifest, user, api_key)
    schema = _safe_schema(manifest["source_path"])
    if schema is None:
        raise HTTPException(status_code=502, detail="Kunne ikke lese schema fra Delta-tabellen")
    return schema


@app.get("/api/products/{product_id}/pipeline", tags=["products"], summary="Siste pipeline-kjøring")
def api_get_pipeline(product_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    manifest = _resolve_product(product_id)
    user     = auth.get_current_user(request)
    _require_access(manifest, user, api_key)
    return _safe_pipeline(manifest.get("dag_id"))


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


@app.get("/api/products/{product_id}/sla", tags=["products"], summary="SLA-ferskhets-status")
def api_get_sla(product_id: str, request: Request, api_key: str | None = Security(_api_key_scheme)):
    manifest = _resolve_product(product_id)
    user     = auth.get_current_user(request)
    _require_access(manifest, user, api_key)
    result = _safe_sla(product_id)
    if result is None:
        result = _compute_sla_live(manifest)
    return result


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
def api_get_product(product_id: str):
    manifest = _resolve_product(product_id)
    return {"manifest": manifest, "history": list_versions(product_id)}


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


# ── UI: HTML-sider ─────────────────────────────────────────────────────────────

def _template_ctx(request: Request, **kwargs) -> dict:
    """Felles malkontekst med innlogget bruker."""
    return {"request": request, "current_user": auth.get_current_user(request), **kwargs}


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
    return templates.TemplateResponse(
        "admin.html",
        _template_ctx(request, users=users, access_requests=requests, pending_count=len(pending)),
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
def page_browse(request: Request):
    user = auth.get_current_user(request)
    if not user:
        return RedirectResponse("/login?next=/browse", status_code=303)
    return templates.TemplateResponse("browse.html", _template_ctx(request))


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
    return templates.TemplateResponse("catalog.html", _template_ctx(
        request,
        products=products,
        domains=domains,
        all_tags=all_tags,
        jupyter_url=JUPYTER_URL,
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

    raw_history       = list_versions(product_id)
    versioned_history = []
    for i, entry in enumerate(raw_history):
        prev = raw_history[i + 1]["manifest"] if i + 1 < len(raw_history) else {}
        versioned_history.append({
            **entry,
            "changes": _diff_manifests(prev, entry["manifest"]),
        })

    schema   = _safe_schema(manifest["source_path"]) if has_access else None
    quality  = _safe_quality(product_id) if has_access else None
    pipeline = _safe_pipeline(manifest.get("dag_id"))
    sla      = (_safe_sla(product_id) or _compute_sla_live(manifest)) if has_access else None

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

    # Sjekk om notebook allerede finnes
    nb_filename     = _safe_filename(product_id)
    nb_exists       = _notebook_path(nb_filename).exists()
    jupyter_nb_url  = _jupyter_open_url(nb_filename) if nb_exists else None

    return templates.TemplateResponse("product.html", _template_ctx(
        request,
        manifest=manifest,
        history=versioned_history,
        schema=schema,
        quality=quality,
        pipeline=pipeline,
        sla=sla,
        airflow_url=AIRFLOW_URL.replace("airflow-webserver", "localhost").replace(":8080", ":8081"),
        has_access=has_access,
        is_restricted=is_restricted,
        pending_request=pending_request,
        nb_filename=nb_filename,
        jupyter_nb_url=jupyter_nb_url,
        jupyter_url=JUPYTER_URL,
        source_product_manifests=source_product_manifests,
    ))


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
        last_run = _safe_pipeline(dag_id)
        timeline = _get_dag_timeline(dag_id)
        dags.append({"dag_id": dag_id, "products": product_names, "last_run": last_run, "timeline": timeline})

    airflow_ui = AIRFLOW_URL.replace("airflow-webserver", "localhost").replace(":8080", ":8081")
    return templates.TemplateResponse("pipelines.html", _template_ctx(
        request,
        dags=dags,
        airflow_ui=airflow_ui,
    ))
