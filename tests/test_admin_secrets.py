"""Unit tests for dataportal/admin_secrets.py."""

from __future__ import annotations

import base64
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "dataportal"))

from admin_secrets import (  # noqa: E402
    MANAGED_KEYS,
    ManagedKey,
    _annotation_keys,
    deployment_for,
    list_managed_keys,
    update_key,
)


# ─── Statisk konfig ───────────────────────────────────────────────────────────


def test_anthropic_key_is_managed():
    assert "anthropic-api-key" in MANAGED_KEYS
    k = MANAGED_KEYS["anthropic-api-key"]
    assert k.env_var == "ANTHROPIC_API_KEY"
    assert k.deployment == "dataportal"
    assert "console.anthropic.com" in k.docs_url


def test_annotation_keys_pattern():
    by, at = _annotation_keys("foo-bar")
    assert by == "slettix.io/foo-bar.last-updated-by"
    assert at == "slettix.io/foo-bar.last-updated-at"


def test_deployment_for_known():
    assert deployment_for("anthropic-api-key") == "dataportal"


def test_deployment_for_unknown():
    assert deployment_for("missing") is None


# ─── list_managed_keys — med mock secret ──────────────────────────────────────


def _fake_secret(value: str | None, by: str | None = None, at: str | None = None) -> dict:
    secret: dict = {"data": {}, "metadata": {"annotations": {}}}
    if value is not None:
        secret["data"]["anthropic-api-key"] = base64.b64encode(value.encode()).decode()
    if by:
        secret["metadata"]["annotations"]["slettix.io/anthropic-api-key.last-updated-by"] = by
    if at:
        secret["metadata"]["annotations"]["slettix.io/anthropic-api-key.last-updated-at"] = at
    return secret


def test_list_reports_unset_when_empty():
    with patch("admin_secrets._fetch_secret", return_value=_fake_secret(None)):
        out = list_managed_keys()
    assert len(out) == 1
    assert out[0]["name"] == "anthropic-api-key"
    assert out[0]["is_set"] is False
    assert out[0]["last_updated_by"] is None


def test_list_reports_set_when_value_present():
    with patch("admin_secrets._fetch_secret", return_value=_fake_secret("sk-ant-xxx", "alice", "2026-01-01T00:00:00Z")):
        out = list_managed_keys()
    assert out[0]["is_set"] is True
    assert out[0]["last_updated_by"] == "alice"
    assert out[0]["last_updated_at"] == "2026-01-01T00:00:00Z"


def test_list_reports_unset_when_only_whitespace():
    # Tomme/whitespace-only verdier skal regnes som ikke satt
    with patch("admin_secrets._fetch_secret", return_value=_fake_secret("   ")):
        out = list_managed_keys()
    assert out[0]["is_set"] is False


def test_list_never_exposes_value():
    with patch("admin_secrets._fetch_secret", return_value=_fake_secret("secret-value-here")):
        out = list_managed_keys()
    serialized = str(out)
    assert "secret-value-here" not in serialized
    assert "sk-" not in serialized


def test_list_when_not_in_cluster_returns_unset_entries():
    with patch("admin_secrets._fetch_secret", return_value=None):
        out = list_managed_keys()
    assert out[0]["is_set"] is False
    assert out[0]["last_updated_by"] is None


# ─── update_key — validering ─────────────────────────────────────────────────


def test_update_unknown_key():
    ok, msg = update_key("does-not-exist", "value", "alice", "2026-01-01T00:00:00Z")
    assert ok is False
    assert "Ukjent" in msg


def test_update_empty_value():
    ok, msg = update_key("anthropic-api-key", "", "alice", "2026-01-01T00:00:00Z")
    assert ok is False
    assert "tom" in msg.lower()


def test_update_whitespace_only_value():
    ok, msg = update_key("anthropic-api-key", "   ", "alice", "2026-01-01T00:00:00Z")
    assert ok is False


def test_update_returns_skipped_when_no_k8s_token():
    with patch("admin_secrets._read_token", return_value=None):
        ok, msg = update_key("anthropic-api-key", "sk-ant-xxx", "alice", "2026-01-01T00:00:00Z")
    assert ok is False
    assert "k8s-token" in msg or "cluster" in msg


def test_update_encodes_value_and_calls_patch():
    captured = {}
    class MockResp:
        status_code = 200
        text = ""
    def mock_patch(*args, **kwargs):
        captured["url"]    = args[0] if args else kwargs.get("url")
        captured["json"]   = kwargs.get("json")
        captured["headers"] = kwargs.get("headers")
        return MockResp()

    with patch("admin_secrets._read_token", return_value="dummy-token"):
        with patch("admin_secrets._current_namespace", return_value="ns"):
            with patch("admin_secrets.httpx.patch", side_effect=mock_patch):
                ok, msg = update_key("anthropic-api-key", "sk-ant-actual-key", "alice", "2026-05-28T10:00:00Z")

    assert ok is True
    # Verifiser at verdien er base64-encoded i body
    assert captured["json"]["data"]["anthropic-api-key"] == base64.b64encode(b"sk-ant-actual-key").decode()
    # Verifiser at audit-annotations er med
    ann = captured["json"]["metadata"]["annotations"]
    assert ann["slettix.io/anthropic-api-key.last-updated-by"] == "alice"
    assert ann["slettix.io/anthropic-api-key.last-updated-at"] == "2026-05-28T10:00:00Z"
    # Verifiser at vi bruker strategic-merge-patch
    assert captured["headers"]["Content-Type"] == "application/strategic-merge-patch+json"


def test_update_handles_k8s_4xx_response():
    class Mock403:
        status_code = 403
        text = "forbidden"
    with patch("admin_secrets._read_token", return_value="dummy-token"):
        with patch("admin_secrets.httpx.patch", return_value=Mock403()):
            ok, msg = update_key("anthropic-api-key", "sk-ant-xxx", "alice", "2026-01-01T00:00:00Z")
    assert ok is False
    assert "403" in msg
