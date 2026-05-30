"""Unit tests for dataportal/agent_charts.py (AGENT-6, epic #165)."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "dataportal"))

from agent_charts import (  # noqa: E402
    CATEGORICAL_MAX_DISTINCT,
    MAX_DF_ROWS,
    SCATTER_MAX_ROWS,
    _classify_columns,
    _is_date,
    label_map_from_manifest,
    recommend_chart,
)


# ─── _is_date ─────────────────────────────────────────────────────────────────


def test_is_date_pandas_datetime():
    s = pd.Series(pd.to_datetime(["2024-01-01", "2024-02-01"]))
    assert _is_date(s) is True


def test_is_date_string_dates():
    s = pd.Series(["2024-01-01", "2024-02-01", "2024-03-01"])
    assert _is_date(s) is True


def test_is_date_integers():
    assert _is_date(pd.Series([1, 2, 3])) is False


def test_is_date_strings_non_dates():
    assert _is_date(pd.Series(["foo", "bar", "baz"])) is False


def test_is_date_empty():
    assert _is_date(pd.Series([], dtype=object)) is False


# ─── _classify_columns ────────────────────────────────────────────────────────


def test_classify_distinguishes_types():
    df = pd.DataFrame({
        "dato":    pd.to_datetime(["2024-01-01", "2024-02-01"]),
        "antall":  [10, 20],
        "kommune": ["Oslo", "Bergen"],
    })
    cols = _classify_columns(df)
    assert "dato" in cols["date"]
    assert "antall" in cols["numeric"]
    assert "kommune" in cols["categorical"]


def test_classify_high_cardinality_string_not_categorical():
    # En streng-kolonne med veldig mange distinkte verdier er ikke kategorisk
    df = pd.DataFrame({"id": [f"a{i}" for i in range(100)]})
    cols = _classify_columns(df)
    assert "id" not in cols["categorical"]


# ─── recommend_chart — regelvalg ──────────────────────────────────────────────


def test_recommend_none_for_empty():
    assert recommend_chart(pd.DataFrame()) is None


def test_recommend_none_for_no_useful_columns():
    df = pd.DataFrame({"id": [f"x{i}" for i in range(100)]})  # bare høy-kardinalitet
    assert recommend_chart(df) is None


def test_recommend_line_for_date_and_numeric():
    df = pd.DataFrame({
        "dato":   pd.date_range("2024-01-01", periods=12, freq="MS"),
        "antall": [10, 20, 15, 30, 25, 40, 35, 50, 45, 60, 55, 70],
    })
    result = recommend_chart(df)
    assert result is not None
    chart_type, png = result
    assert chart_type == "line"
    assert png.startswith(b"\x89PNG")


def test_recommend_bar_for_categorical_and_numeric():
    df = pd.DataFrame({
        "kommune": ["Oslo", "Bergen", "Trondheim", "Stavanger", "Tromsø"],
        "antall":  [100, 80, 50, 40, 20],
    })
    result = recommend_chart(df)
    assert result is not None
    chart_type, png = result
    assert chart_type == "bar"
    assert png.startswith(b"\x89PNG")


def test_recommend_scatter_for_two_numeric_small():
    df = pd.DataFrame({"x": list(range(100)), "y": [i * 2 for i in range(100)]})
    result = recommend_chart(df)
    assert result is not None
    chart_type, png = result
    assert chart_type == "scatter"
    assert png.startswith(b"\x89PNG")


def test_recommend_hexbin_for_two_numeric_large():
    n = SCATTER_MAX_ROWS + 100
    df = pd.DataFrame({"x": list(range(n)), "y": list(range(n))})
    result = recommend_chart(df)
    assert result is not None
    chart_type, png = result
    assert chart_type == "hexbin"


def test_recommend_histogram_for_single_numeric():
    df = pd.DataFrame({"alder": [25, 30, 35, 40, 45, 50, 55, 60, 65, 70]})
    result = recommend_chart(df)
    assert result is not None
    chart_type, png = result
    assert chart_type == "histogram"


def test_recommend_prioritizes_date_over_categorical():
    df = pd.DataFrame({
        "kommune": ["Oslo"] * 12,
        "dato":    pd.date_range("2024-01-01", periods=12, freq="MS"),
        "antall":  list(range(12)),
    })
    result = recommend_chart(df)
    chart_type, _ = result
    assert chart_type == "line"


# ─── Sampling for store DataFrames ────────────────────────────────────────────


def test_recommend_samples_large_dataframe(monkeypatch):
    # Lag 100k+1 rader — funksjonen skal sample uten å feile
    n = MAX_DF_ROWS + 100
    df = pd.DataFrame({"alder": list(range(n))})
    result = recommend_chart(df)
    assert result is not None  # ikke krasjer på store DataFrames


# ─── label_map ────────────────────────────────────────────────────────────────


def test_label_map_from_manifest_basic():
    manifest = {"schema": [
        {"name": "kommune", "type": "string", "label_no": "Kommune"},
        {"name": "antall",  "type": "integer", "label_no": "Antall dødsfall"},
        {"name": "id",      "type": "string"},  # ingen label_no
    ]}
    out = label_map_from_manifest(manifest)
    assert out == {"kommune": "Kommune", "antall": "Antall dødsfall"}


def test_label_map_from_manifest_empty_schema():
    assert label_map_from_manifest({}) == {}
    assert label_map_from_manifest({"schema": []}) == {}


def test_label_map_applied_to_chart_axes():
    df = pd.DataFrame({"k": ["a", "b"], "v": [1, 2]})
    lm = {"k": "Kategorisk", "v": "Verdi"}
    # Sjekk at recommend_chart aksepterer label_map uten å krasje
    result = recommend_chart(df, label_map=lm)
    assert result is not None
