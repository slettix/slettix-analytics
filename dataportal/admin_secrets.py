"""
admin_secrets — administrer K8s-Secret-baserte API-nøkler fra admin-UI.

V1 håndterer kun ANTHROPIC_API_KEY, men datastrukturen er utvidbar: legg til
nye nøkler i MANAGED_KEYS uten kode-endring andre steder. Bare admin-rolle
kan kalle endepunktene (sjekkes i main.py).

Lagring:
  - Verdier ligger i k8s-secreten `slettix-credentials` (samme som i dag)
  - Audit-metadata (sist endret av/når) lagres som annotations på samme secret
    så de overlever pod-restart uten ekstern DB

Sikkerhet:
  - Verdiene returneres ALDRI fra GET-endepunktet
  - PUT-loggingen viser key-navn + bruker, ikke selve verdien
"""

from __future__ import annotations

import base64
import json
import logging
import os
import pathlib
from dataclasses import dataclass

import httpx


log = logging.getLogger(__name__)


_K8S_API_BASE       = "https://kubernetes.default.svc"
_K8S_TOKEN_FILE     = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_K8S_CA_FILE        = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
_K8S_NAMESPACE_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

SECRET_NAME = "slettix-credentials"


@dataclass(frozen=True)
class ManagedKey:
    """Et administrerbart secret-felt i slettix-credentials."""
    secret_key:  str   # nøkkelen i k8s Secret data (kebab-case)
    env_var:     str   # tilsvarende env-variabel i dataportal-poden
    label:       str   # bruker-vendt navn
    description: str   # forklarer hva den brukes til + hvor man får den
    docs_url:    str   # ekstern lenke for å hente nøkkelen
    deployment:  str   # k8s-deployment som må restartes etter endring


# Utvidbar liste — legg til nye nøkler her uten å endre kode andre steder.
MANAGED_KEYS: dict[str, ManagedKey] = {
    "anthropic-api-key": ManagedKey(
        secret_key=  "anthropic-api-key",
        env_var=     "ANTHROPIC_API_KEY",
        label=       "Anthropic API Key",
        description= "Brukes av agent-veiviseren for å kalle Claude. "
                     "Krever kreditter på Anthropic-kontoen.",
        docs_url=    "https://console.anthropic.com/settings/keys",
        deployment=  "dataportal",
    ),
}


# ─── Hjelpere ─────────────────────────────────────────────────────────────────


def _read_token() -> str | None:
    try:
        return pathlib.Path(_K8S_TOKEN_FILE).read_text().strip()
    except FileNotFoundError:
        return None


def _current_namespace() -> str:
    try:
        return pathlib.Path(_K8S_NAMESPACE_FILE).read_text().strip()
    except FileNotFoundError:
        return os.environ.get("K8S_NAMESPACE", "slettix-analytics")


def _annotation_keys(secret_key: str) -> tuple[str, str]:
    """Returnerer (last_updated_by_annotation, last_updated_at_annotation)."""
    return (
        f"slettix.io/{secret_key}.last-updated-by",
        f"slettix.io/{secret_key}.last-updated-at",
    )


# ─── Lese-API ─────────────────────────────────────────────────────────────────


def _fetch_secret() -> dict | None:
    """Henter slettix-credentials-secreten fra k8s-API. Returnerer None hvis
    vi ikke er i cluster eller kallet feiler."""
    token = _read_token()
    if not token:
        return None
    try:
        resp = httpx.get(
            f"{_K8S_API_BASE}/api/v1/namespaces/{_current_namespace()}/secrets/{SECRET_NAME}",
            headers={"Authorization": f"Bearer {token}"},
            verify=_K8S_CA_FILE,
            timeout=10.0,
        )
        if resp.status_code >= 400:
            log.warning("Kunne ikke hente secret %s: %s", SECRET_NAME, resp.status_code)
            return None
        return resp.json()
    except Exception as exc:
        log.warning("Feil ved henting av secret: %s", exc)
        return None


def list_managed_keys() -> list[dict]:
    """Returnerer status per administrert nøkkel — uten å eksponere verdier.

    Hver post inneholder: name, label, description, docs_url, env_var,
    is_set, last_updated_by, last_updated_at.
    """
    secret = _fetch_secret() or {}
    data        = secret.get("data") or {}
    annotations = (secret.get("metadata") or {}).get("annotations") or {}

    out: list[dict] = []
    for name, key in MANAGED_KEYS.items():
        ann_by, ann_at = _annotation_keys(key.secret_key)
        raw_value      = data.get(key.secret_key, "")
        # Tom verdi (eller bare whitespace etter base64-dekoding) = ikke satt
        try:
            decoded = base64.b64decode(raw_value).decode() if raw_value else ""
        except Exception:
            decoded = ""
        out.append({
            "name":             name,
            "label":            key.label,
            "description":      key.description,
            "docs_url":         key.docs_url,
            "env_var":          key.env_var,
            "deployment":       key.deployment,
            "is_set":           bool(decoded.strip()),
            "last_updated_by":  annotations.get(ann_by),
            "last_updated_at":  annotations.get(ann_at),
        })
    return out


# ─── Skrive-API ──────────────────────────────────────────────────────────────


def update_key(name: str, value: str, updated_by: str, now_iso: str) -> tuple[bool, str]:
    """Oppdaterer en administrert nøkkel + setter audit-annotations.

    Returnerer (success, message). Krever rolle med
    `secrets[slettix-credentials]/get,patch` i namespacet.
    """
    if name not in MANAGED_KEYS:
        return False, f"Ukjent nøkkel: {name}"
    key = MANAGED_KEYS[name]
    if not value or not value.strip():
        return False, "Verdien kan ikke være tom"

    token = _read_token()
    if not token:
        return False, "Ikke i cluster — k8s-token mangler"

    ann_by, ann_at = _annotation_keys(key.secret_key)
    encoded        = base64.b64encode(value.strip().encode()).decode()

    # Strategic-merge-patch: oppdaterer både data og annotations atomisk
    patch = {
        "metadata": {
            "annotations": {
                ann_by: updated_by,
                ann_at: now_iso,
            },
        },
        "data": {
            key.secret_key: encoded,
        },
    }

    try:
        resp = httpx.patch(
            f"{_K8S_API_BASE}/api/v1/namespaces/{_current_namespace()}/secrets/{SECRET_NAME}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type":  "application/strategic-merge-patch+json",
            },
            json=patch,
            verify=_K8S_CA_FILE,
            timeout=10.0,
        )
        if resp.status_code >= 400:
            return False, f"k8s-patch feilet: {resp.status_code} {resp.text[:200]}"
    except Exception as exc:
        return False, f"k8s-patch unntak: {exc}"

    return True, "ok"


def deployment_for(name: str) -> str | None:
    """Returnerer hvilken deployment som må restartes etter at nøkkelen er endret."""
    if name not in MANAGED_KEYS:
        return None
    return MANAGED_KEYS[name].deployment
