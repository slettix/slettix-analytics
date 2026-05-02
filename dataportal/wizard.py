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

    # Felles boilerplate
    md("## 1. Last data")
    code(
        "from pyspark.sql import SparkSession, functions as F\n"
        "spark = (\n"
        "    SparkSession.builder.appName('analyze')\n"
        "    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')\n"
        "    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')\n"
        "    .getOrCreate()\n"
        ")\n\n"
        f"df = spark.read.format('delta').load('{source}')\n"
        f"print(f'{{df.count():,}} rader, {{len(df.columns)}} kolonner')\n"
        "df.printSchema()"
    )

    if template_key == "cohort":
        first_cat = col_names[0] if col_names else "kategori"
        md(f"## 2. Cohort-analyse — fordeling per `{first_cat}`")
        code(
            f"cohort = df.groupBy('{first_cat}').count().orderBy(F.desc('count'))\n"
            "cohort.show(20)"
        )
        md("## 3. Visualisering")
        code(
            "import matplotlib.pyplot as plt\n"
            f"pd_cohort = cohort.limit(20).toPandas()\n"
            f"pd_cohort.plot(kind='bar', x='{first_cat}', y='count', figsize=(12, 4), legend=False)\n"
            f"plt.title('Fordeling per {first_cat}')\n"
            "plt.tight_layout()"
        )
    elif template_key == "timeseries":
        time_cols = [c for c in col_names if any(s in c.lower() for s in ("date", "time", "_at", "ts"))]
        time_col = time_cols[0] if time_cols else "event_date"
        metric_col = next((c for c in col_names if c not in time_cols and c != "person_id"), col_names[0] if col_names else "value")
        md(f"## 2. Aggregér per måned (`{time_col}`)")
        code(
            f"ts = (\n"
            f"    df.withColumn('period', F.date_trunc('month', F.col('{time_col}')))\n"
            f"      .groupBy('period')\n"
            f"      .agg(F.count('*').alias('antall'))\n"
            f"      .orderBy('period')\n"
            ")\n"
            "ts.show(24)"
        )
        md("## 3. Plot tidsserien")
        code(
            "import matplotlib.pyplot as plt\n"
            "pd_ts = ts.toPandas()\n"
            "plt.figure(figsize=(12, 4))\n"
            "plt.plot(pd_ts['period'], pd_ts['antall'], marker='o')\n"
            f"plt.title('Antall per måned')\n"
            "plt.xticks(rotation=45)\n"
            "plt.tight_layout()"
        )
    elif template_key == "distribution":
        numeric_hint = next((c for c in col_names if any(s in c.lower() for s in ("count", "amount", "value", "score", "days", "hours"))), col_names[0] if col_names else "value")
        md(f"## 2. Deskriptiv statistikk for `{numeric_hint}`")
        code(
            f"df.select('{numeric_hint}').describe().show()"
        )
        md("## 3. Histogram")
        code(
            "import matplotlib.pyplot as plt\n"
            f"vals = df.select('{numeric_hint}').toPandas()['{numeric_hint}'].dropna()\n"
            f"plt.figure(figsize=(10, 4))\n"
            f"plt.hist(vals, bins=30)\n"
            f"plt.title('Fordeling — {numeric_hint}')\n"
            f"plt.xlabel('{numeric_hint}')\n"
            f"plt.tight_layout()"
        )
    else:  # explore
        md("## 2. Inspeksjon")
        code("df.show(5)")
        md("## 3. Null-rate per kolonne")
        code(
            "from pyspark.sql.functions import col, sum as _sum, when\n"
            "null_counts = df.select([\n"
            "    _sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns\n"
            "])\n"
            "null_counts.show(vertical=True)"
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
