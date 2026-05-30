"""Unit tests for dataportal/agent_orchestrator.py (AGENT-1 + AGENT-7 helpers)."""

from __future__ import annotations

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "dataportal"))

from agent_orchestrator import (  # noqa: E402
    AgentResponse,
    DEFAULT_MODEL_KEY,
    MODEL_CHOICES,
    ROW_COUNT_LIMIT,
    RequestContext,
    _ContextCache,
    _RowCountCache,
    _extract_json,
    _validate_response_shape,
    build_notebook,
    cache_get,
    cache_put,
    cache_size,
    call_claude,
    extract_generated_code,
    make_request_id,
    resolve_model_id,
)


# ─── resolve_model_id ─────────────────────────────────────────────────────────


def test_resolve_model_id_haiku():
    assert resolve_model_id("haiku") == "claude-haiku-4-5-20251001"


def test_resolve_model_id_sonnet():
    assert resolve_model_id("sonnet") == "claude-sonnet-4-6"


def test_resolve_model_id_opus():
    assert resolve_model_id("opus") == "claude-opus-4-7"


def test_resolve_model_id_uppercase_normalized():
    assert resolve_model_id("SONNET") == "claude-sonnet-4-6"


def test_resolve_model_id_none_uses_default():
    assert resolve_model_id(None) == MODEL_CHOICES[DEFAULT_MODEL_KEY]["model_id"]


def test_resolve_model_id_invalid_raises():
    with pytest.raises(ValueError, match="Ugyldig modellvalg"):
        resolve_model_id("gpt-4")


def test_model_choices_have_tooltip():
    for key, info in MODEL_CHOICES.items():
        assert "tooltip" in info and len(info["tooltip"]) > 20, f"{key} mangler tooltip"


# ─── _ContextCache ────────────────────────────────────────────────────────────


def _make_ctx(rid="abc"):
    return RequestContext(
        request_id=rid, user_id="u1", product_ids=["p.a"], product_versions={"p.a": "1.0.0"},
        question="?", model_key="sonnet", model_id="claude-sonnet-4-6",
        system_prompt="sys", generated_code="code", answer_preview="prev",
    )


def test_cache_put_and_get():
    c = _ContextCache(ttl_seconds=300)
    ctx = _make_ctx("rid1")
    c.put(ctx)
    assert c.get("rid1") is ctx
    assert c.get("missing") is None


def test_cache_expires_after_ttl():
    c = _ContextCache(ttl_seconds=300)
    ctx = _make_ctx("rid2")
    # Sett created_at langt tilbake i tid
    ctx.created_at = time.time() - 1000
    c.put(ctx)
    # get utløser gc — ctx skal være borte
    assert c.get("rid2") is None


def test_cache_thread_safe_basic():
    import threading
    c = _ContextCache(ttl_seconds=300)
    def worker(i):
        c.put(_make_ctx(f"r{i}"))
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(50)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert len(c) == 50


def test_module_cache_helpers():
    rid = make_request_id()
    ctx = _make_ctx(rid)
    cache_put(ctx)
    assert cache_get(rid) is ctx
    assert cache_size() >= 1


def test_make_request_id_unique():
    ids = {make_request_id() for _ in range(100)}
    assert len(ids) == 100
    assert all(len(i) == 16 for i in ids)


# ─── JSON-ekstraksjon og validering ───────────────────────────────────────────


def test_extract_json_from_raw():
    txt = '{"cells": [], "summary": "x"}'
    assert _extract_json(txt) == {"cells": [], "summary": "x"}


def test_extract_json_from_fence():
    txt = 'Her er svaret:\n```json\n{"cells": [], "summary": "x"}\n```\nFerdig.'
    assert _extract_json(txt) == {"cells": [], "summary": "x"}


def test_extract_json_from_fence_no_lang():
    txt = '```\n{"cells": [], "summary": "y"}\n```'
    assert _extract_json(txt) == {"cells": [], "summary": "y"}


def test_extract_json_invalid_raises():
    with pytest.raises(ValueError, match="Klarte ikke å parse"):
        _extract_json("ikke JSON i det hele tatt")


def test_validate_shape_ok():
    data = {
        "cells": [
            {"type": "markdown", "content": "# hei"},
            {"type": "code",     "content": "print(1)"},
        ],
        "summary": "ok",
    }
    _validate_response_shape(data)  # skal ikke kaste


def test_validate_shape_missing_cells():
    with pytest.raises(ValueError, match="Mangler 'cells'"):
        _validate_response_shape({"summary": "x"})


def test_validate_shape_invalid_cell_type():
    with pytest.raises(ValueError, match="ugyldig type"):
        _validate_response_shape({
            "cells":   [{"type": "raw", "content": "x"}],
            "summary": "",
        })


def test_validate_shape_cell_without_content_string():
    with pytest.raises(ValueError, match="content"):
        _validate_response_shape({
            "cells":   [{"type": "code", "content": 123}],
            "summary": "",
        })


def test_validate_shape_summary_defaults_to_empty():
    data = {"cells": [{"type": "markdown", "content": "x"}]}
    _validate_response_shape(data)
    assert data["summary"] == ""


# ─── call_claude med mock ─────────────────────────────────────────────────────


def _make_mock_anthropic(reply_text: str, input_tokens=100, output_tokens=200):
    """Bygg en mock-Anthropic-klient som returnerer en gitt tekst."""
    block       = MagicMock(type="text", text=reply_text)
    usage       = MagicMock(input_tokens=input_tokens, output_tokens=output_tokens)
    message     = MagicMock(content=[block], usage=usage)
    client      = MagicMock()
    client.messages.create = MagicMock(return_value=message)
    return lambda: client


def test_call_claude_parses_response():
    reply = '{"cells": [{"type": "code", "content": "df = get_product(\'x\')"}], "summary": "fant 5 rader"}'
    resp = call_claude(
        api_key="dummy",
        model_id="x",
        system_prompt="sys",
        question="q",
        client_factory=_make_mock_anthropic(reply, 150, 80),
    )
    assert isinstance(resp, AgentResponse)
    assert resp.cells[0]["type"] == "code"
    assert resp.summary == "fant 5 rader"
    assert resp.input_tokens == 150
    assert resp.output_tokens == 80


def test_call_claude_strips_markdown_fence():
    reply = 'her er:\n```json\n{"cells": [{"type": "markdown", "content": "ok"}], "summary": ""}\n```\n'
    resp = call_claude(
        api_key="dummy",
        model_id="x",
        system_prompt="sys",
        question="q",
        client_factory=_make_mock_anthropic(reply),
    )
    assert resp.cells == [{"type": "markdown", "content": "ok"}]


def test_call_claude_raises_on_bad_json():
    with pytest.raises(ValueError):
        call_claude(
            api_key="dummy",
            model_id="x",
            system_prompt="sys",
            question="q",
            client_factory=_make_mock_anthropic("definitivt ikke json"),
        )


# ─── build_notebook ───────────────────────────────────────────────────────────


def test_build_notebook_structure():
    cells = [{"type": "code", "content": "x = 1"}]
    nb = build_notebook(cells, "spm?", ["p.a"])
    assert nb["nbformat"] == 4
    assert nb["metadata"]["slettix_agent"]["product_ids"] == ["p.a"]
    # Vi prepender header + setup-celler, så minst 3 celler totalt
    assert len(nb["cells"]) >= 3


def test_build_notebook_header_contains_question_and_products():
    nb = build_notebook(
        [{"type": "code", "content": "x = 1"}],
        "Hvor mange rader er det i x?",
        ["p.a", "p.b"],
    )
    header_md = "".join(nb["cells"][0]["source"])
    assert "Hvor mange rader" in header_md
    assert "p.a" in header_md and "p.b" in header_md


def test_build_notebook_setup_imports_slettix_client():
    nb = build_notebook([{"type": "code", "content": "x=1"}], "q", ["p"])
    setup_src = "".join(nb["cells"][1]["source"])
    assert "from slettix_client import get_product" in setup_src


def test_build_notebook_preserves_cell_order():
    cells = [
        {"type": "markdown", "content": "# en"},
        {"type": "code",     "content": "x = 1"},
        {"type": "markdown", "content": "# to"},
    ]
    nb = build_notebook(cells, "q", ["p"])
    # Etter header + setup, kommer våre 3 celler i samme rekkefølge
    types_after_prelude = [c["cell_type"] for c in nb["cells"][2:]]
    assert types_after_prelude == ["markdown", "code", "markdown"]


# ─── extract_generated_code ───────────────────────────────────────────────────


def test_extract_code_joins_only_code_cells():
    cells = [
        {"type": "markdown", "content": "# Tittel"},
        {"type": "code",     "content": "x = 1"},
        {"type": "markdown", "content": "## Resultat"},
        {"type": "code",     "content": "print(x)"},
    ]
    out = extract_generated_code(cells)
    assert "x = 1" in out and "print(x)" in out
    assert "Tittel" not in out
    assert "Resultat" not in out


def test_extract_code_empty_when_no_code():
    assert extract_generated_code([{"type": "markdown", "content": "x"}]) == ""


# ─── _RowCountCache (AGENT-4) ─────────────────────────────────────────────────


def test_row_count_cache_put_and_get():
    c = _RowCountCache(ttl_seconds=300)
    c.put("s3a://x/y", 42)
    assert c.get("s3a://x/y") == 42
    assert c.get("missing") is None


def test_row_count_cache_expires():
    c = _RowCountCache(ttl_seconds=0)  # umiddelbar utløp
    c.put("s3a://x/y", 42)
    time.sleep(0.01)
    assert c.get("s3a://x/y") is None


def test_row_count_cache_clear():
    c = _RowCountCache()
    c.put("a", 1)
    c.put("b", 2)
    c.clear()
    assert c.get("a") is None and c.get("b") is None


def test_row_count_limit_constant():
    # MVP-grensen er 1M rader — flagge ved evt. utilsiktet endring
    assert ROW_COUNT_LIMIT == 1_000_000
