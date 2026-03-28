"""
Data product registry backed by a Delta Lake table.

The registry stores one row per published manifest version, append-only.
Each call to register() adds a new row — version history is always preserved.

Reads and writes use delta-rs (no Spark session required), making the registry
suitable for use in the portal backend, CLI tools and Airflow PythonOperators.

Storage layout:
    s3://gold/data_products/   (Delta table)
    Columns:
        product_id    – "hr.employees"
        domain        – "hr"
        name          – "Employees"
        owner         – "hr-team"
        version       – "1.0.0"
        access        – "public" | "restricted"
        manifest_json – full manifest serialised as JSON string
        registered_at – ISO-8601 UTC timestamp

Usage:
    from registry import register, get, list_all, list_versions

    register(manifest_dict)
    products = list_all()            # latest version per product
    manifest = get("hr.employees")  # latest manifest as dict
    history  = list_versions("hr.employees")
"""

import json
import os
from datetime import datetime, timezone

import pyarrow as pa
from deltalake import DeltaTable
from deltalake import write_deltalake

REGISTRY_PATH = os.environ.get("REGISTRY_PATH", "s3://gold/data_products")

_STORAGE_OPTIONS: dict = {
    "AWS_ENDPOINT_URL":           os.environ.get("MINIO_ENDPOINT",    "http://minio:9000"),
    "AWS_ACCESS_KEY_ID":          os.environ.get("MINIO_ACCESS_KEY",  "admin"),
    "AWS_SECRET_ACCESS_KEY":      os.environ.get("MINIO_SECRET_KEY",  "changeme"),
    "AWS_ALLOW_HTTP":             "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

_SCHEMA = pa.schema([
    pa.field("product_id",    pa.string(), nullable=False),
    pa.field("domain",        pa.string(), nullable=False),
    pa.field("name",          pa.string(), nullable=False),
    pa.field("owner",         pa.string(), nullable=False),
    pa.field("version",       pa.string(), nullable=False),
    pa.field("access",        pa.string(), nullable=False),
    pa.field("manifest_json", pa.string(), nullable=False),
    pa.field("registered_at", pa.string(), nullable=False),
])


def register(manifest: dict) -> None:
    """
    Publish *manifest* to the registry.

    Appends a new row regardless of whether the product already exists —
    the previous version is preserved in the table history.

    Args:
        manifest: A validated data product manifest dict.
    """
    row = {
        "product_id":    manifest["id"],
        "domain":        manifest["domain"],
        "name":          manifest["name"],
        "owner":         manifest["owner"],
        "version":       manifest["version"],
        "access":        manifest.get("access", "public"),
        "manifest_json": json.dumps(manifest),
        "registered_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    table = pa.Table.from_pylist([row], schema=_SCHEMA)
    write_deltalake(
        REGISTRY_PATH,
        table,
        mode="append",
        storage_options=_STORAGE_OPTIONS,
    )


def list_all() -> list[dict]:
    """
    Return the latest published manifest for every data product.

    Returns an empty list if the registry does not yet exist.
    """
    try:
        dt = DeltaTable(REGISTRY_PATH, storage_options=_STORAGE_OPTIONS)
    except Exception:
        return []

    df = dt.to_pandas()
    # Keep only the most recently registered row per product
    df = (
        df.sort_values("registered_at", ascending=False)
          .drop_duplicates(subset=["product_id"], keep="first")
    )
    return [json.loads(row) for row in df["manifest_json"]]


def get(product_id: str) -> dict:
    """
    Return the latest manifest for *product_id*.

    Raises KeyError if the product is not in the registry.
    """
    products = {p["id"]: p for p in list_all()}
    if product_id not in products:
        raise KeyError(f"Data product '{product_id}' not found in registry")
    return products[product_id]


def list_versions(product_id: str) -> list[dict]:
    """
    Return all published versions of *product_id*, newest first.

    Each entry: {"version": ..., "registered_at": ..., "manifest": {...}}
    """
    try:
        dt = DeltaTable(REGISTRY_PATH, storage_options=_STORAGE_OPTIONS)
    except Exception:
        return []

    df = dt.to_pandas()
    df = (
        df[df["product_id"] == product_id]
        .sort_values("registered_at", ascending=False)
    )
    return [
        {
            "version":       row["version"],
            "registered_at": row["registered_at"],
            "manifest":      json.loads(row["manifest_json"]),
        }
        for _, row in df.iterrows()
    ]
