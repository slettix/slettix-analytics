#!/usr/bin/env python3
"""
publish.py — Publiser et dataprodukt til Slettix Analytics-registeret.

Bruk:
    python jobs/publish.py --manifest conf/products/hr_employees.json

Hva den gjør:
  1. Leser og validerer manifestet mot JSON-skjema
  2. Utleder schema automatisk fra Delta-tabellen i MinIO
  3. Auto-inkrementerer versjon om nødvendig
  4. Publiserer til produkt-registeret (Delta-tabell i s3://gold/data_products)

Miljøvariabler (valgfrie, med standardverdier):
  MINIO_ENDPOINT    http://localhost:9000
  MINIO_ACCESS_KEY  admin
  MINIO_SECRET_KEY  changeme
  PORTAL_URL        http://localhost:8090
"""

import argparse
import json
import os
import sys
from pathlib import Path

# ── avhengigheter ──────────────────────────────────────────────────────────────

try:
    from deltalake import DeltaTable
    import jsonschema
except ImportError as exc:
    print(f"Mangler avhengighet: {exc}")
    print("Kjør: pip install deltalake jsonschema pyarrow")
    sys.exit(1)

# registry.py ligger i samme mappe
sys.path.insert(0, str(Path(__file__).parent))
from registry import get, list_all, register  # noqa: E402

# ── konfigurasjon ──────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "changeme")
PORTAL_URL       = os.environ.get("PORTAL_URL",       "http://localhost:8090")

_STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL":           MINIO_ENDPOINT,
    "AWS_ACCESS_KEY_ID":          MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY":      MINIO_SECRET_KEY,
    "AWS_ALLOW_HTTP":             "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

_SCHEMA_PATH = (
    Path(__file__).parent.parent
    / "conf" / "products" / "schema" / "data_product_schema.json"
)

# ── hjelpefunksjoner ───────────────────────────────────────────────────────────

def _load_json_schema() -> dict:
    if not _SCHEMA_PATH.exists():
        raise FileNotFoundError(f"JSON-skjema ikke funnet: {_SCHEMA_PATH}")
    return json.loads(_SCHEMA_PATH.read_text())


def _validate(manifest: dict) -> None:
    """Valider manifestet mot JSON-skjema. Kaster SystemExit ved feil."""
    try:
        jsonschema.validate(instance=manifest, schema=_load_json_schema())
    except jsonschema.ValidationError as exc:
        print(f"\n✗ Valideringsfeil i manifest:")
        print(f"  Felt : {' → '.join(str(p) for p in exc.absolute_path) or '(rot)'}")
        print(f"  Feil : {exc.message}")
        sys.exit(1)


def _read_delta_schema(source_path: str) -> list[dict]:
    """
    Les Delta-tabellens schema fra MinIO og returner som liste av feltdicts.
    Inkluderer TBLPROPERTIES-kommentarer som kolonnebeskrivelser.
    """
    dt = DeltaTable(source_path, storage_options=_STORAGE_OPTIONS)
    raw_fields = json.loads(dt.schema().to_json()).get("fields", [])

    # Delta-tabellen kan ha column comments i metadata["comment"]
    table_meta = dt.metadata()
    col_comments: dict[str, str] = {}
    if table_meta.configuration:
        for key, val in table_meta.configuration.items():
            # Spark lagrer kolonne-kommentarer som "spark.sql.sources.schema.part.0" etc.
            # Vi sjekker også "comment.<kolonne>" som noen verktøy bruker
            if key.startswith("comment."):
                col_comments[key[len("comment."):]] = val

    fields = []
    for f in raw_fields:
        field: dict = {
            "name":     f["name"],
            "type":     str(f["type"]),
            "nullable": f.get("nullable", True),
        }
        comment = (
            col_comments.get(f["name"])
            or (f.get("metadata") or {}).get("comment")
        )
        if comment:
            field["description"] = comment
        fields.append(field)
    return fields


def _version_tuple(v: str) -> tuple[int, ...]:
    return tuple(int(x) for x in v.split("."))


def _bump_patch(v: str) -> str:
    major, minor, patch = v.split(".")
    return f"{major}.{minor}.{int(patch) + 1}"


def _resolve_version(product_id: str, manifest_version: str) -> str:
    """
    Bestem publiseringsversjon:
    - Hvis produktet ikke finnes i registeret: bruk manifest-versjon
    - Hvis manifest-versjon > siste registrerte versjon: bruk manifest-versjon (bevisst bump)
    - Ellers: auto-inkrementer patch fra siste registrerte versjon
    """
    try:
        current = get(product_id)
        latest  = current["version"]
    except KeyError:
        return manifest_version  # nytt produkt

    if _version_tuple(manifest_version) > _version_tuple(latest):
        return manifest_version   # brukeren har gjort en bevisst versjonsbump

    return _bump_patch(latest)    # auto-inkrementer patch


# ── main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publiser et dataprodukt til Slettix Analytics-registeret.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--manifest", required=True,
        help="Sti til manifest-JSON (f.eks. conf/products/hr_employees.json)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Valider og vis hva som ville blitt publisert, uten å skrive til registeret",
    )
    parser.add_argument(
        "--type", choices=["source", "analytical"], default=None,
        dest="product_type",
        help="Produkttype: source (standard) eller analytical (konsument-skapt sammenstilling)",
    )
    args = parser.parse_args()

    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        print(f"✗ Manifest-fil ikke funnet: {manifest_path}")
        sys.exit(1)

    # 1. Les manifest
    try:
        manifest = json.loads(manifest_path.read_text())
    except json.JSONDecodeError as exc:
        print(f"✗ Ugyldig JSON i manifest: {exc}")
        sys.exit(1)

    print(f"→ Leser manifest: {manifest_path}")
    print(f"  Produkt : {manifest.get('id', '?')}")
    print(f"  Type    : {manifest.get('product_type', 'source')}")
    print(f"  Versjon : {manifest.get('version', '?')} (i manifest)")

    # Sett product_type fra CLI hvis oppgitt (CLI overstyrer manifest)
    if args.product_type:
        manifest["product_type"] = args.product_type
    manifest.setdefault("product_type", "source")

    # 2. Valider mot JSON-skjema (uten schema-felt som vi legger til etterpå)
    manifest_for_validation = {k: v for k, v in manifest.items() if k != "schema"}
    _validate(manifest_for_validation)
    print("  ✓ Manifest-validering OK")

    # 3. Les Delta-schema
    source_path = manifest["source_path"]
    print(f"→ Leser Delta-schema fra {source_path} ...")
    try:
        schema_fields = _read_delta_schema(source_path)
        manifest["schema"] = schema_fields
        print(f"  ✓ {len(schema_fields)} kolonner funnet: {', '.join(f['name'] for f in schema_fields)}")
    except Exception as exc:
        print(f"  ! Kunne ikke lese Delta-schema: {exc}")
        print("    Schema settes ikke i manifestet — fortsetter uten.")

    # 4. Bestem versjon
    published_version = _resolve_version(manifest["id"], manifest["version"])
    manifest["version"] = published_version
    if published_version != manifest.get("version"):
        print(f"→ Versjon auto-inkrementert til {published_version}")
    else:
        print(f"→ Publiserer som versjon {published_version}")

    # 5. Dry-run: vis og avslutt
    if args.dry_run:
        print("\n── Dry-run: manifest som ville blitt publisert ──")
        print(json.dumps(manifest, indent=2, ensure_ascii=False))
        print("\n(Ingen endringer skrevet — kjør uten --dry-run for å publisere)")
        return

    # 6. Publiser
    print("→ Publiserer til registeret ...")
    register(manifest)

    product_url = f"{PORTAL_URL}/products/{manifest['id']}"
    print(f"\n✓ Publisert!")
    print(f"  Produkt-ID : {manifest['id']}")
    print(f"  Versjon    : {published_version}")
    print(f"  Portal     : {product_url}")


if __name__ == "__main__":
    main()
