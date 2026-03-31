"""
slettix_client — enkel Python-klient for Slettix Analytics Data Portal.

Gir analytikere tilgang til dataprodukter uten å kjenne underliggende
stier, format eller lagringsteknologi.

Bruk (i Jupyter):
    import sys
    sys.path.insert(0, "/home/spark/jobs")
    from slettix_client import list_products, get_product

    products = list_products()
    df = get_product("hr.employees")
"""

import os

import pandas as pd
import requests
from deltalake import DeltaTable

PORTAL_URL       = os.environ.get("PORTAL_URL",        "http://dataportal:8090")
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",    "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY",  "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY",  "changeme")

_STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL":           MINIO_ENDPOINT,
    "AWS_ACCESS_KEY_ID":          MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY":      MINIO_SECRET_KEY,
    "AWS_ALLOW_HTTP":             "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}


def _headers(api_key: str | None) -> dict:
    return {"X-API-Key": api_key} if api_key else {}


def list_products(api_key: str | None = None) -> list[dict]:
    """
    Hent liste over alle tilgjengelige dataprodukter fra portalen.

    Args:
        api_key: Valgfri API-nøkkel for å inkludere restricted-produkter.

    Returns:
        Liste av manifest-dicts, ett per produkt.
    """
    resp = requests.get(
        f"{PORTAL_URL}/api/products",
        headers=_headers(api_key),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def get_manifest(product_id: str, api_key: str | None = None) -> dict:
    """
    Hent manifest for ett dataprodukt.

    Args:
        product_id: Produkt-ID (f.eks. 'hr.employees').
        api_key:    Valgfri API-nøkkel for restricted-produkter.

    Returns:
        Manifest-dict.

    Raises:
        PermissionError: Hvis produktet er restricted og ingen gyldig nøkkel er oppgitt.
        KeyError:        Hvis produktet ikke finnes.
    """
    resp = requests.get(
        f"{PORTAL_URL}/api/products/{product_id}",
        headers=_headers(api_key),
        timeout=10,
    )
    if resp.status_code == 403:
        raise PermissionError(
            f"Produktet '{product_id}' er restricted. Oppgi api_key."
        )
    if resp.status_code == 404:
        raise KeyError(f"Produktet '{product_id}' finnes ikke i registeret.")
    resp.raise_for_status()
    return resp.json()["manifest"]


def get_product(
    product_id: str,
    api_key: str | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """
    Last et dataprodukt som pandas DataFrame.

    Henter manifest fra portalen, leser Delta-tabellen direkte fra MinIO
    og returnerer data som en pandas DataFrame.

    Args:
        product_id: Produkt-ID (f.eks. 'hr.employees').
        api_key:    Valgfri API-nøkkel for restricted-produkter.
        columns:    Valgfri liste med kolonner å inkludere (projection pushdown).

    Returns:
        pandas DataFrame med produktets data.

    Example:
        >>> df = get_product("hr.employees")
        >>> df.head()
    """
    manifest = get_manifest(product_id, api_key)
    source   = manifest["source_path"]

    print(f"Laster '{manifest['name']}' fra {source} ...")
    dt = DeltaTable(source, storage_options=_STORAGE_OPTIONS)

    if columns:
        df = dt.to_pandas(columns=columns)
    else:
        df = dt.to_pandas()

    print(f"✓ {len(df)} rader, {len(df.columns)} kolonner")
    return df


def get_sla(product_id: str, api_key: str | None = None) -> dict:
    """
    Hent SLA-status for et dataprodukt fra portalen.

    Returns:
        Dict med feltene: compliant, last_updated, hours_since_update, freshness_hours.
    """
    resp = requests.get(
        f"{PORTAL_URL}/api/products/{product_id}/sla",
        headers=_headers(api_key),
        timeout=10,
    )
    if resp.status_code == 403:
        raise PermissionError(f"Ingen tilgang til '{product_id}'.")
    resp.raise_for_status()
    return resp.json()


def get_quality(product_id: str, api_key: str | None = None) -> dict:
    """Hent siste GE-valideringsresultat for et dataprodukt."""
    resp = requests.get(
        f"{PORTAL_URL}/api/products/{product_id}/quality",
        headers=_headers(api_key),
        timeout=10,
    )
    if resp.status_code == 403:
        raise PermissionError(f"Ingen tilgang til '{product_id}'.")
    resp.raise_for_status()
    return resp.json()
