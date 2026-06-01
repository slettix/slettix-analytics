"""Unit tests for dataportal/agent.py (epic #165, AGENT-2 + AGENT-3 helpers)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "dataportal"))

from agent import (  # noqa: E402
    build_agent_prompt,
    find_candidate_gold_products,
    has_pii,
    pii_columns,
    _estimate_tokens,
    _is_gold_path,
)


# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def manifest_plain():
    return {
        "id":          "test.plain",
        "name":        "Test plain",
        "domain":      "test",
        "owner":       "test-team",
        "description": "Et helt vanlig produkt uten PII.",
        "source_path": "s3a://silver/test/plain",
        "schema": [
            {"name": "id",     "type": "string",  "pii": False, "description": "primærnøkkel"},
            {"name": "kommune","type": "string",  "pii": False},
            {"name": "antall", "type": "integer", "pii": False},
        ],
    }


@pytest.fixture
def manifest_pii():
    return {
        "id":          "test.persons",
        "name":        "Test persons",
        "domain":      "test",
        "owner":       "test-team",
        "source_path": "s3a://silver/test/persons",
        "schema": [
            {"name": "person_id", "type": "string", "pii": True, "sensitivity": "high"},
            {"name": "fnr",       "type": "string", "pii": True, "sensitivity": "critical"},
            {"name": "kommune",   "type": "string", "pii": False},
        ],
    }


@pytest.fixture
def sample_plain():
    return {
        "format":    "flat",
        "row_count": 100,
        "columns": [
            {"name": "id",     "type": "string", "null_rate": 0.0, "sample_values": ["A1", "A2", "A3"], "pii_hint": False},
            {"name": "kommune","type": "string", "null_rate": 0.0, "sample_values": ["Oslo", "Bergen"],    "pii_hint": False},
            {"name": "antall", "type": "int64",  "null_rate": 0.0, "sample_values": [10, 20, 30],          "pii_hint": False},
        ],
    }


# ─── has_pii ──────────────────────────────────────────────────────────────────


def test_has_pii_returns_false_for_plain(manifest_plain):
    assert has_pii([manifest_plain]) is False


def test_has_pii_returns_true_if_any_field_marked(manifest_pii):
    assert has_pii([manifest_pii]) is True


def test_has_pii_returns_true_if_any_product_has_pii(manifest_plain, manifest_pii):
    assert has_pii([manifest_plain, manifest_pii]) is True


def test_has_pii_returns_false_for_empty_list():
    assert has_pii([]) is False


def test_has_pii_handles_missing_schema():
    assert has_pii([{"id": "x"}]) is False


def test_has_pii_handles_schema_without_pii_field():
    # Gamle manifester kan mangle pii-flagg helt
    assert has_pii([{"id": "x", "schema": [{"name": "col"}]}]) is False


# ─── pii_columns ──────────────────────────────────────────────────────────────


def test_pii_columns_returns_empty_for_plain(manifest_plain):
    assert pii_columns([manifest_plain]) == {}


def test_pii_columns_returns_pii_field_names(manifest_pii):
    out = pii_columns([manifest_pii])
    assert out == {"test.persons": ["person_id", "fnr"]}


def test_pii_columns_only_includes_products_with_pii(manifest_plain, manifest_pii):
    out = pii_columns([manifest_plain, manifest_pii])
    assert "test.plain" not in out
    assert out["test.persons"] == ["person_id", "fnr"]


# ─── build_agent_prompt ───────────────────────────────────────────────────────


def test_build_prompt_raises_on_empty_manifests():
    with pytest.raises(ValueError, match="ingen manifester"):
        build_agent_prompt([], {}, "spørsmål")


def test_build_prompt_raises_on_empty_question(manifest_plain):
    with pytest.raises(ValueError, match="tomt spørsmål"):
        build_agent_prompt([manifest_plain], {}, "   ")


def test_build_prompt_contains_question(manifest_plain):
    p = build_agent_prompt([manifest_plain], {}, "hvor mange rader er det?")
    assert "hvor mange rader er det?" in p


def test_build_prompt_contains_product_id_and_schema(manifest_plain):
    p = build_agent_prompt([manifest_plain], {}, "spm")
    assert "test.plain" in p
    assert "kommune" in p
    assert "primærnøkkel" in p  # description leveres inn


def test_build_prompt_marks_pii_columns(manifest_pii):
    p = build_agent_prompt([manifest_pii], {}, "spm")
    # Vi markerer pii-kolonner med [PII]-tag i schema-listen
    assert "[PII]" in p
    assert "fnr" in p


def test_build_prompt_includes_sample_when_provided(manifest_plain, sample_plain):
    p = build_agent_prompt(
        [manifest_plain],
        {"test.plain": sample_plain},
        "spm",
    )
    # Sample-verdier skal være med
    assert "Oslo" in p
    assert "Bergen" in p


def test_build_prompt_handles_missing_sample(manifest_plain):
    # Hvis ingen sample finnes, skal vi fortsatt få en gyldig prompt
    p = build_agent_prompt([manifest_plain], {}, "spm")
    assert "ingen sample-data tilgjengelig" in p or "test.plain" in p


def test_build_prompt_includes_system_preamble(manifest_plain):
    p = build_agent_prompt([manifest_plain], {}, "spm")
    assert "slettix_client" in p
    assert "get_product" in p


def test_build_prompt_truncates_when_oversized(manifest_plain, sample_plain):
    # Tving veldig lav token-grense — sample skal dropps, men manifest beholdes
    p = build_agent_prompt(
        [manifest_plain],
        {"test.plain": sample_plain},
        "spm",
        max_tokens=50,  # umulig lavt
    )
    # Manifest-id må fortsatt være med (det er det viktigste — ikke hallusinasjon)
    assert "test.plain" in p
    # Men sample-verdier skal være borte
    assert "Oslo" not in p


def test_build_prompt_token_estimate_reasonable_for_three_products(
    manifest_plain, manifest_pii, sample_plain
):
    # Tre produkter med sample-data skal være innenfor 10k tokens
    third = {**manifest_plain, "id": "test.third"}
    p = build_agent_prompt(
        [manifest_plain, manifest_pii, third],
        {"test.plain": sample_plain, "test.third": sample_plain},
        "et spørsmål",
    )
    assert _estimate_tokens(p) <= 10_000


# ─── find_candidate_gold_products (AGENT-4) ──────────────────────────────────


def test_is_gold_path():
    assert _is_gold_path("s3://gold/x/y") is True
    assert _is_gold_path("s3a://gold/x/y") is True
    assert _is_gold_path("s3://silver/x/y") is False
    assert _is_gold_path("s3a://bronze/x/y") is False
    assert _is_gold_path(None) is False
    assert _is_gold_path("") is False


def test_find_candidates_returns_only_same_domain_gold():
    selected = [{"id": "x.bronze", "domain": "helse", "source_path": "s3a://bronze/x"}]
    all_products = [
        {"id": "x.gold.kommune",  "domain": "helse",  "source_path": "s3a://gold/x/kommune"},
        {"id": "x.gold.fylke",    "domain": "helse",  "source_path": "s3://gold/x/fylke"},
        {"id": "x.silver",        "domain": "helse",  "source_path": "s3a://silver/x"},  # silver droppes
        {"id": "y.gold",          "domain": "annen",  "source_path": "s3a://gold/y"},     # annen domene
        {"id": "x.bronze",        "domain": "helse",  "source_path": "s3a://bronze/x"},   # selv-referanse
    ]
    out = find_candidate_gold_products(selected, all_products)
    ids = [p["id"] for p in out]
    assert "x.gold.kommune" in ids
    assert "x.gold.fylke"   in ids
    assert "x.silver"       not in ids
    assert "y.gold"         not in ids
    assert "x.bronze"       not in ids


def test_find_candidates_truncates_to_max_results():
    selected = [{"id": "x", "domain": "helse", "source_path": "s3a://silver/x"}]
    all_products = [
        {"id": f"gold.{i}", "domain": "helse", "source_path": f"s3a://gold/x{i}"}
        for i in range(10)
    ]
    out = find_candidate_gold_products(selected, all_products, max_results=3)
    assert len(out) == 3


def test_find_candidates_returns_description_truncated():
    selected = [{"id": "x", "domain": "helse"}]
    all_products = [{
        "id": "g.1", "domain": "helse", "source_path": "s3a://gold/g",
        "description": "x" * 300,
    }]
    out = find_candidate_gold_products(selected, all_products)
    assert len(out[0]["description"]) == 200


def test_find_candidates_handles_missing_domain():
    selected = [{"id": "x"}]  # ingen domain
    all_products = [{"id": "g", "domain": "helse", "source_path": "s3a://gold/g"}]
    assert find_candidate_gold_products(selected, all_products) == []
