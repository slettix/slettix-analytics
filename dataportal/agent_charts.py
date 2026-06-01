"""
agent_charts — regelbasert visualiserings-recommender (AGENT-6, epic #165).

Tar en pandas DataFrame fra et agent-svar og foreslår én chart-type basert
på kolonnetyper. Returnerer (chart_type, png_bytes). Ingen ekstra LLM-kall.

Regler (i prioritert rekkefølge):
  1. Én datokolonne + numerisk → linjegraf
  2. Én kategorisk (< 20 distinkte) + numerisk → bar
  3. To numeriske → scatter (< 5k rader) eller hexbin
  4. Én numerisk → histogram
  5. Ellers → None (ingen meningsfull chart)

Bruker matplotlib med Agg-backend (headless). Ingen X11-avhengighet.

V1-avgrensning: integrasjon med AGENT-1 venter på at notebook-eksekvering er
implementert. Funksjonen er testet selvstendig og kan plugges inn når
DataFrames er tilgjengelige server-side.
"""

from __future__ import annotations

import io
import logging
from typing import Any

import matplotlib
matplotlib.use("Agg")  # headless — settes FØR pyplot importeres
import matplotlib.pyplot as plt
import pandas as pd
from pandas.api import types as ptypes


log = logging.getLogger(__name__)


# Maks DataFrame-størrelse før vi sampler. >100k rader fortsatt blir til en
# meningsfull visualisering ved sampling — full plotting blir tregt og produkter
# en bunke unnyttige pixels.
MAX_DF_ROWS = 100_000

# Kategorisk = kolonne med max så mange distinkte verdier (utover er det
# i praksis ikke meningsfullt som x-akse).
CATEGORICAL_MAX_DISTINCT = 20

# Scatter vs hexbin: med flere enn dette går vi over til hexbin for å unngå
# overplotting.
SCATTER_MAX_ROWS = 5_000


def _to_label(col_name: str, label_map: dict[str, str] | None) -> str:
    if label_map and col_name in label_map:
        return label_map[col_name]
    return col_name


def _is_date(series: pd.Series) -> bool:
    if ptypes.is_datetime64_any_dtype(series):
        return True
    # Bare prøv parsing for object/string-kolonner — tall-kolonner er aldri datoer
    if not (ptypes.is_object_dtype(series) or ptypes.is_string_dtype(series)):
        return False
    non_null = series.dropna()
    if non_null.empty:
        return False
    try:
        # Sample-baseret heuristikk: minst 80% av verdiene parses som dato
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            parsed = pd.to_datetime(non_null.head(50), errors="coerce")
        return bool(parsed.notna().mean() >= 0.8)
    except Exception:
        return False


def _classify_columns(df: pd.DataFrame) -> dict[str, list[str]]:
    """Returnerer {date: [...], numeric: [...], categorical: [...]}."""
    date_cols:        list[str] = []
    numeric_cols:     list[str] = []
    categorical_cols: list[str] = []
    for col in df.columns:
        s = df[col]
        if _is_date(s):
            date_cols.append(col)
        elif ptypes.is_numeric_dtype(s):
            numeric_cols.append(col)
        elif s.nunique(dropna=True) <= CATEGORICAL_MAX_DISTINCT:
            categorical_cols.append(col)
    return {"date": date_cols, "numeric": numeric_cols, "categorical": categorical_cols}


def _fig_to_png(fig) -> bytes:
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=100, bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()


def recommend_chart(
    df: pd.DataFrame,
    label_map: dict[str, str] | None = None,
) -> tuple[str, bytes] | None:
    """Hovedinngang. Returnerer (chart_type, png_bytes) eller None.

    chart_type er én av: "line", "bar", "scatter", "hexbin", "histogram".

    label_map: {kolonnenavn: norsk_label} — typisk hentet fra manifestets
    schema (felt `label_no`). Aksene merkes med disse hvis tilgjengelig,
    ellers brukes rå kolonnenavn.
    """
    if df is None or df.empty:
        return None

    # Sample store DataFrames
    if len(df) > MAX_DF_ROWS:
        log.info("Sampler DataFrame fra %d til %d rader for chart", len(df), MAX_DF_ROWS)
        df = df.sample(n=MAX_DF_ROWS, random_state=42)

    cols = _classify_columns(df)
    date_cols = cols["date"]
    num_cols  = cols["numeric"]
    cat_cols  = cols["categorical"]

    try:
        # Regel 1: dato + numerisk → linje
        if date_cols and num_cols:
            return _plot_line(df, date_cols[0], num_cols[0], label_map)
        # Regel 2: kategorisk + numerisk → bar
        if cat_cols and num_cols:
            return _plot_bar(df, cat_cols[0], num_cols[0], label_map)
        # Regel 3: to numeriske → scatter eller hexbin
        if len(num_cols) >= 2:
            x, y = num_cols[0], num_cols[1]
            if len(df) <= SCATTER_MAX_ROWS:
                return _plot_scatter(df, x, y, label_map)
            return _plot_hexbin(df, x, y, label_map)
        # Regel 4: én numerisk → histogram
        if len(num_cols) == 1:
            return _plot_histogram(df, num_cols[0], label_map)
    except Exception as exc:
        log.warning("Chart-rendering feilet: %s", exc)
        return None

    return None


def _plot_line(df: pd.DataFrame, x: str, y: str, lm) -> tuple[str, bytes]:
    # Sørg for at x er datetime og sorter
    data = df[[x, y]].copy()
    data[x] = pd.to_datetime(data[x], errors="coerce")
    data = data.dropna().sort_values(x)
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(data[x], data[y], linewidth=1.2)
    ax.set_xlabel(_to_label(x, lm))
    ax.set_ylabel(_to_label(y, lm))
    ax.set_title(f"{_to_label(y, lm)} over tid")
    fig.autofmt_xdate()
    return "line", _fig_to_png(fig)


def _plot_bar(df: pd.DataFrame, cat: str, num: str, lm) -> tuple[str, bytes]:
    grouped = df.groupby(cat)[num].sum().sort_values(ascending=False).head(CATEGORICAL_MAX_DISTINCT)
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(grouped.index.astype(str), grouped.values, color="#0d6efd")
    ax.set_xlabel(_to_label(cat, lm))
    ax.set_ylabel(_to_label(num, lm))
    ax.set_title(f"{_to_label(num, lm)} per {_to_label(cat, lm)}")
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    return "bar", _fig_to_png(fig)


def _plot_scatter(df: pd.DataFrame, x: str, y: str, lm) -> tuple[str, bytes]:
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.scatter(df[x], df[y], alpha=0.5, s=15, color="#0d6efd")
    ax.set_xlabel(_to_label(x, lm))
    ax.set_ylabel(_to_label(y, lm))
    ax.set_title(f"{_to_label(y, lm)} vs {_to_label(x, lm)}")
    return "scatter", _fig_to_png(fig)


def _plot_hexbin(df: pd.DataFrame, x: str, y: str, lm) -> tuple[str, bytes]:
    fig, ax = plt.subplots(figsize=(7, 5))
    hb = ax.hexbin(df[x], df[y], gridsize=30, cmap="Blues")
    fig.colorbar(hb, ax=ax, label="antall")
    ax.set_xlabel(_to_label(x, lm))
    ax.set_ylabel(_to_label(y, lm))
    ax.set_title(f"{_to_label(y, lm)} vs {_to_label(x, lm)} (tetthet)")
    return "hexbin", _fig_to_png(fig)


def _plot_histogram(df: pd.DataFrame, col: str, lm) -> tuple[str, bytes]:
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.hist(df[col].dropna(), bins=30, color="#0d6efd", edgecolor="white")
    ax.set_xlabel(_to_label(col, lm))
    ax.set_ylabel("antall")
    ax.set_title(f"Fordeling av {_to_label(col, lm)}")
    return "histogram", _fig_to_png(fig)


def label_map_from_manifest(manifest: dict) -> dict[str, str]:
    """Bygg {kolonnenavn: label_no} fra manifestets schema. Tom dict hvis ingen.
    Brukes for å gi norske akser når DataFrame-kolonnene matcher manifestets."""
    out: dict[str, str] = {}
    for field in (manifest.get("schema") or []):
        name  = field.get("name")
        label = field.get("label_no")
        if name and label:
            out[name] = label
    return out
