"""
GUIDE-5/6: veivisere fra spørsmål til analyse, og fra produkt til dashboard.

Bygger på eksisterende generate_product_notebook-mønster, men gir analytikeren
en valgmeny av analyse-templates (cohort, tidsserie, fordeling, ad-hoc) som
fyller notebooken med ferdig boilerplate. Slik slipper brukeren å starte fra
en blank notebook og kopiere paths/credentials manuelt.
"""

ANALYSIS_TEMPLATES = [
    {
        "key":         "cohort",
        "title":       "Cohort-analyse",
        "description": "Grupper rader etter en kategorisk dimensjon og se utviklingen.",
        "icon":        "bi-people",
    },
    {
        "key":         "timeseries",
        "title":       "Tidsserie",
        "description": "Beregn aggregater per tidsperiode og visualisér trenden.",
        "icon":        "bi-graph-up",
    },
    {
        "key":         "distribution",
        "title":       "Fordeling",
        "description": "Histogrammer og deskriptiv statistikk for én eller flere kolonner.",
        "icon":        "bi-bar-chart",
    },
    {
        "key":         "explore",
        "title":       "Utforsk",
        "description": "Generell mal — last data, vis schema, første rader, null-rate.",
        "icon":        "bi-search",
    },
]


def get_template(key: str) -> dict | None:
    for t in ANALYSIS_TEMPLATES:
        if t["key"] == key:
            return t
    return None


def build_notebook(manifest: dict, template_key: str, question: str = "") -> dict:
    """Bygg en .ipynb-dict for valgt analyse-template og dataprodukt.

    Notebook-strukturen følger Jupyter-formatet og kan pushes via samme
    `_push_to_jupyter()`-mønster som eksisterende notebook-generering.
    """
    pid         = manifest.get("id", "ukjent")
    name        = manifest.get("name", pid)
    source      = manifest.get("source_path", "")
    schema      = manifest.get("schema") or []
    col_names   = [c["name"] for c in schema if c.get("name")]
    pii_cols    = [c["name"] for c in schema if c.get("pii")]
    domain      = manifest.get("domain", "")
    description = manifest.get("description", "")

    nb = {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {"name": "python3", "display_name": "Python 3"},
            "language_info": {"name": "python"},
        },
        "cells": [],
    }

    def md(text: str):
        nb["cells"].append({"cell_type": "markdown", "metadata": {}, "source": text})

    def code(src: str):
        nb["cells"].append({
            "cell_type": "code", "metadata": {}, "execution_count": None,
            "outputs": [], "source": src,
        })

    md(f"# {name} — {get_template(template_key)['title']}")
    if question:
        md(f"**Spørsmål du analyserer:** {question}")
    if description:
        md(description)
    if pii_cols:
        md(f"⚠️ **PII-kolonner**: `{', '.join(pii_cols)}` — håndter forsiktig.")

    # Felles boilerplate (bruker slettix_client → pandas DataFrame, samme
    # pattern som /products/{id}-notebook-generatoren — unngår Spark-k8s-config)
    md("## 1. Last data")
    code(
        "import sys\n"
        "sys.path.insert(0, '/home/spark/jobs')\n"
        "from slettix_client import get_product\n"
        "\n"
        f"PRODUCT_ID = '{pid}'\n"
        "df = get_product(PRODUCT_ID)\n"
        "print(f'{len(df):,} rader, {len(df.columns)} kolonner')\n"
        "df.head()"
    )

    if template_key == "cohort":
        first_cat = col_names[0] if col_names else "kategori"
        md(f"## 2. Cohort-analyse — fordeling per `{first_cat}`")
        code(
            f"cohort = df.groupby('{first_cat}').size().reset_index(name='count').sort_values('count', ascending=False)\n"
            "cohort.head(20)"
        )
        md("## 3. Visualisering")
        code(
            "import matplotlib.pyplot as plt\n"
            f"cohort.head(20).plot(kind='bar', x='{first_cat}', y='count', figsize=(12, 4), legend=False)\n"
            f"plt.title('Fordeling per {first_cat}')\n"
            "plt.tight_layout()"
        )
    elif template_key == "timeseries":
        time_cols = [c for c in col_names if any(s in c.lower() for s in ("date", "time", "_at", "ts"))]
        time_col = time_cols[0] if time_cols else "event_date"
        md(f"## 2. Aggregér per måned (`{time_col}`)")
        code(
            "import pandas as pd\n"
            f"df['{time_col}'] = pd.to_datetime(df['{time_col}'])\n"
            f"ts = (df.set_index('{time_col}')\n"
            f"        .groupby(pd.Grouper(freq='M'))\n"
            f"        .size()\n"
            f"        .reset_index(name='antall'))\n"
            "ts.head(24)"
        )
        md("## 3. Plot tidsserien")
        code(
            "import matplotlib.pyplot as plt\n"
            "plt.figure(figsize=(12, 4))\n"
            f"plt.plot(ts['{time_col}'], ts['antall'], marker='o')\n"
            "plt.title('Antall per måned')\n"
            "plt.xticks(rotation=45)\n"
            "plt.tight_layout()"
        )
    elif template_key == "distribution":
        numeric_hint = next((c for c in col_names if any(s in c.lower() for s in ("count", "amount", "value", "score", "days", "hours"))), col_names[0] if col_names else "value")
        md(f"## 2. Deskriptiv statistikk for `{numeric_hint}`")
        code(
            f"df['{numeric_hint}'].describe()"
        )
        md("## 3. Histogram")
        code(
            "import matplotlib.pyplot as plt\n"
            f"vals = df['{numeric_hint}'].dropna()\n"
            "plt.figure(figsize=(10, 4))\n"
            "plt.hist(vals, bins=30)\n"
            f"plt.title('Fordeling — {numeric_hint}')\n"
            f"plt.xlabel('{numeric_hint}')\n"
            "plt.tight_layout()"
        )
    else:  # explore
        md("## 2. Inspeksjon")
        code("df.head(10)")
        md("## 3. Null-rate per kolonne")
        code(
            "null_rate = (df.isnull().mean() * 100).round(1).sort_values(ascending=False)\n"
            "null_rate.to_frame('null_pct')"
        )

    md("---")
    md(f"*Generert via veiviseren for {pid} ({domain}). Tilpass og fortsett analysen.*")
    return nb


# ── GUIDE-6: dashboard-veiviser ────────────────────────────────────────────────

DASHBOARD_TYPES = [
    {"key": "overview",  "title": "Oversikt",   "description": "KPI-ruter med antall, snitt og fordeling.", "icon": "bi-speedometer"},
    {"key": "timeseries","title": "Tidsserie",  "description": "Antall eller sum over tid per kategori.",  "icon": "bi-graph-up"},
    {"key": "geo",       "title": "Geografisk", "description": "Verdier kartlagt per kommune (krever municipality_code).", "icon": "bi-geo-alt"},
    {"key": "table",     "title": "Tabell",     "description": "Full tabell-spørring med filtre.",          "icon": "bi-table"},
]


def build_dashboard_sql(manifest: dict, dashboard_type: str, columns: list[str] | None = None) -> str:
    source = manifest.get("source_path", "")
    cols   = columns or [c["name"] for c in (manifest.get("schema") or []) if c.get("name")]
    proj   = ", ".join(cols) if cols else "*"

    if dashboard_type == "overview":
        return (
            f"-- Oversikt: KPI-er for {manifest.get('id')}\n"
            f"SELECT\n"
            f"  COUNT(*) AS total_rader\n"
            f"FROM delta_scan('{source}');"
        )
    if dashboard_type == "timeseries":
        time_cols = [c for c in cols if any(s in c.lower() for s in ("date", "time", "_at", "ts", "period"))]
        time_col  = time_cols[0] if time_cols else "event_date"
        return (
            f"-- Tidsserie: antall per måned\n"
            f"SELECT\n"
            f"  date_trunc('month', {time_col}) AS periode,\n"
            f"  COUNT(*) AS antall\n"
            f"FROM delta_scan('{source}')\n"
            f"GROUP BY 1 ORDER BY 1;"
        )
    if dashboard_type == "geo":
        return (
            f"-- Geografisk: verdier per kommune\n"
            f"SELECT\n"
            f"  municipality_code,\n"
            f"  municipality_name,\n"
            f"  COUNT(*) AS antall\n"
            f"FROM delta_scan('{source}')\n"
            f"GROUP BY municipality_code, municipality_name\n"
            f"ORDER BY antall DESC;"
        )
    # table
    return (
        f"-- Tabell-spørring\n"
        f"SELECT {proj}\n"
        f"FROM delta_scan('{source}')\n"
        f"LIMIT 1000;"
    )
