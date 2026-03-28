"""
Validator for data product manifests.

Loads the JSON Schema from conf/products/schema/data_product_schema.json
and validates a manifest dict against it.

Usage:
    from manifest_validator import load_and_validate, validate

    # Validate a dict already in memory
    validate({"id": "hr.employees", ...})

    # Load from file and validate in one step
    manifest = load_and_validate("/opt/airflow/spark_conf/products/hr_employees.json")
"""

import json
import os
from pathlib import Path

import jsonschema

# Default schema path in the Airflow container (conf/ is mounted as spark_conf/).
# Override with the DATA_PRODUCT_SCHEMA_PATH env var when running elsewhere.
_DEFAULT_SCHEMA = "/opt/airflow/spark_conf/products/schema/data_product_schema.json"
SCHEMA_PATH = os.environ.get("DATA_PRODUCT_SCHEMA_PATH", _DEFAULT_SCHEMA)


def _load_schema() -> dict:
    path = Path(SCHEMA_PATH)
    if not path.exists():
        raise FileNotFoundError(
            f"Data product schema not found at '{SCHEMA_PATH}'. "
            "Set DATA_PRODUCT_SCHEMA_PATH env var to override."
        )
    return json.loads(path.read_text())


def validate(manifest: dict) -> None:
    """
    Validate *manifest* against the data product JSON schema.

    Raises jsonschema.ValidationError with a clear message if validation fails.
    """
    schema = _load_schema()
    jsonschema.validate(instance=manifest, schema=schema)


def load_and_validate(manifest_path: str) -> dict:
    """
    Read a manifest JSON file, validate it, and return the parsed dict.

    Raises:
        FileNotFoundError  – manifest file not found
        jsonschema.ValidationError – manifest does not conform to schema
    """
    path = Path(manifest_path)
    if not path.exists():
        raise FileNotFoundError(f"Manifest not found: '{manifest_path}'")
    manifest = json.loads(path.read_text())
    validate(manifest)
    return manifest
