"""
SILVER-1: Schema-introspeksjon av Bronze/IDP-produkter.

Leser et lite sample fra en Delta-tabell og returnerer kolonner med:
  - type
  - null-rate
  - 3-5 eksempel-verdier
  - PII-mistanke basert på kolonnenavn

For IDP-format (kolonne `payload_json` finnes) ekstraheres også top-level
JSON-keys fra de første ikke-null payloadene, slik at wizarden kan foreslå
hvilke felter som skal flates ut til Silver.

Bruker deltalake-rust (samme som slettix_client) — ingen Spark-overhead.
"""

import json
import os
import re
from typing import Any

from deltalake import DeltaTable

_STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL":           os.environ.get("MINIO_ENDPOINT",   "http://minio:9000"),
    "AWS_ACCESS_KEY_ID":          os.environ.get("MINIO_ACCESS_KEY", "admin"),
    "AWS_SECRET_ACCESS_KEY":      os.environ.get("MINIO_SECRET_KEY", "changeme"),
    "AWS_ALLOW_HTTP":             "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

# Kolonnenavn som hinter om PII — verdier maskeres i sample-output.
_PII_HINTS = re.compile(
    r"^(fnr|ssn|email|phone|address|tlf|navn|name|first_?name|last_?name|"
    r"full_?name|birthdate|dob|personnummer)$",
    re.IGNORECASE,
)

# Maks antall eksempel-verdier per kolonne.
_SAMPLE_VALUES = 4


def _is_pii_hint(col: str) -> bool:
    return bool(_PII_HINTS.match(col))


def _sample_values(series, col_name: str, max_n: int = _SAMPLE_VALUES) -> list[Any]:
    """Hent inntil max_n unike ikke-null eksempel-verdier. Maskér ved PII-mistanke."""
    values = [v for v in series.dropna().head(20).tolist()]
    uniq: list[Any] = []
    seen = set()
    for v in values:
        s = str(v)
        if s not in seen:
            seen.add(s)
            uniq.append(v)
        if len(uniq) >= max_n:
            break
    if _is_pii_hint(col_name):
        return ["***" for _ in uniq]
    # Klipp lange strenger så payload ikke svulmer
    return [s[:120] + "…" if isinstance(s, str) and len(s) > 120 else s for s in uniq]


def _extract_payload_fields(payloads: list[str], max_samples: int = 20) -> list[dict]:
    """Parse JSON-payloads og samle top-level keys med eksempel-verdier."""
    fields: dict[str, dict] = {}
    parsed_count = 0
    for raw in payloads:
        if parsed_count >= max_samples:
            break
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except (ValueError, TypeError):
            continue
        if not isinstance(obj, dict):
            continue
        parsed_count += 1
        for key, value in obj.items():
            f = fields.setdefault(key, {
                "key":           key,
                "json_path":     f"$.{key}",
                "inferred_type": "string",
                "samples":       [],
                "non_null":      0,
            })
            if value is None:
                continue
            f["non_null"] += 1
            # Type-inferens
            if isinstance(value, bool):
                f["inferred_type"] = "boolean"
            elif isinstance(value, int):
                f["inferred_type"] = "long"
            elif isinstance(value, float):
                f["inferred_type"] = "double"
            elif isinstance(value, (dict, list)):
                f["inferred_type"] = "string"  # JSON-strukturer holdes som streng i V1
            if len(f["samples"]) < _SAMPLE_VALUES:
                sval = json.dumps(value) if isinstance(value, (dict, list)) else value
                if _is_pii_hint(key):
                    sval = "***"
                if sval not in f["samples"]:
                    f["samples"].append(sval)

    out = []
    for f in fields.values():
        out.append({
            "name":          f["key"],
            "json_path":     f["json_path"],
            "inferred_type": f["inferred_type"],
            "null_rate":     round(1.0 - f["non_null"] / parsed_count, 3) if parsed_count else 1.0,
            "sample_values": f["samples"],
            "pii_hint":      _is_pii_hint(f["key"]),
        })
    return sorted(out, key=lambda x: x["name"])


def introspect(source_path: str, limit: int = 100) -> dict:
    """Returnér `{columns, format, payload_fields}` for et Bronze-produkt.

    `format` er enten "flat" eller "payload_json". I sistnevnte tilfelle
    inneholder `payload_fields` ekstraherte JSON-keys.
    """
    try:
        dt = DeltaTable(source_path, storage_options=_STORAGE_OPTIONS)
        # Bruk pyarrow-dataset for å begrense lesing — to_pandas() ville
        # lastet hele tabellen i minnet (kritisk for store IDP-tabeller).
        import pyarrow as pa
        scanner = dt.to_pyarrow_dataset().scanner(batch_size=limit)
        batches: list[pa.RecordBatch] = []
        rows_read = 0
        for batch in scanner.to_batches():
            batches.append(batch)
            rows_read += batch.num_rows
            if rows_read >= limit:
                break
        if not batches:
            return {"columns": [], "format": "flat", "payload_fields": [], "row_count": 0}
        table = pa.Table.from_batches(batches)
        if table.num_rows > limit:
            table = table.slice(0, limit)
        df = table.to_pandas()
    except Exception as exc:
        return {"error": f"Kunne ikke lese Delta-tabell: {exc}", "columns": [], "format": "flat", "payload_fields": []}

    if len(df) == 0:
        return {"columns": [], "format": "flat", "payload_fields": [], "row_count": 0}

    columns = []
    for col in df.columns:
        s = df[col]
        col_info = {
            "name":          col,
            "type":          str(s.dtype),
            "null_rate":     round(s.isnull().mean(), 3),
            "sample_values": _sample_values(s, col),
            "pii_hint":      _is_pii_hint(col),
        }
        columns.append(col_info)

    fmt = "payload_json" if "payload_json" in df.columns else "flat"
    payload_fields = []
    if fmt == "payload_json":
        payloads = df["payload_json"].dropna().astype(str).tolist()
        payload_fields = _extract_payload_fields(payloads)

    return {
        "format":         fmt,
        "row_count":      len(df),
        "columns":        columns,
        "payload_fields": payload_fields,
    }
