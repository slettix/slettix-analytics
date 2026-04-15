"""
Superset-konfigurasjon for Slettix Analytics.

Kobler Superset mot:
  - SQLite (lokal metadata-database)
  - DuckDB (spørringer mot Delta Lake i MinIO via httpfs + delta-extension)

DuckDB-tilkoblinger konfigureres automatisk via SQLAlchemy pool-hendelse:
hver ny tilkobling laster httpfs/delta og setter S3-parametere mot MinIO.
"""

import os
from sqlalchemy import event
from sqlalchemy.pool import Pool

# ── Hemmeligheter og metadata-database ────────────────────────────────────────
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "slettix-superset-dev-key")
SQLALCHEMY_DATABASE_URI = "sqlite:////var/lib/superset/superset.db"

# Enklere dev-oppsett: slå av CSRF og aktivér template-prosessering i SQL Lab
WTF_CSRF_ENABLED = False
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# ── DuckDB → MinIO S3-konfigurasjon ───────────────────────────────────────────
_minio_host = (
    os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    .replace("https://", "")
    .replace("http://", "")
)
_access_key = os.environ.get("MINIO_ACCESS_KEY", "admin")
_secret_key = os.environ.get("MINIO_SECRET_KEY", "changeme")

_DUCKDB_INIT_SQL = [
    "INSTALL httpfs; LOAD httpfs;",
    "INSTALL delta; LOAD delta;",
    # CREATE SECRET brukes av både httpfs og delta-extension (DeltaKernel)
    # SET s3_* gjelder kun httpfs og ignoreres av DeltaKernel FFI
    f"""
    CREATE OR REPLACE SECRET slettix_minio (
        TYPE S3,
        KEY_ID '{_access_key}',
        SECRET '{_secret_key}',
        REGION 'us-east-1',
        ENDPOINT '{_minio_host}',
        URL_STYLE 'path',
        USE_SSL false
    );
    """,
]


@event.listens_for(Pool, "connect")
def _on_duckdb_connect(dbapi_conn, connection_record):
    """Konfigurer hver ny DuckDB-tilkobling med httpfs og MinIO S3-innstillinger."""
    if "duckdb" not in getattr(type(dbapi_conn), "__module__", ""):
        return
    for sql in _DUCKDB_INIT_SQL:
        try:
            dbapi_conn.execute(sql)
        except Exception:
            pass
