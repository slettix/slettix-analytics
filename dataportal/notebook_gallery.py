"""
GUIDE-8: notebook-galleri.

Sentral registreringsdict over eksempel-notebooks som brukerne kan fork-e
til sitt eget Jupyter-area. Hvert eksempel peker til en .ipynb-fil i
`notebooks_gallery/`-katalogen.

Når brukeren klikker «Fork til mitt Jupyter»:
  1. Genererer kopinavn (`<slug>-<timestamp>.ipynb`)
  2. Kopierer .ipynb-filen til NOTEBOOKS_DIR
  3. Pusher til Jupyter via samme `_push_to_jupyter()` som genererte notebooks
  4. Returnerer Jupyter-URL i ny fane

For å legge til et nytt eksempel:
  1. Lag .ipynb-fil i `dataportal/notebooks_gallery/`
  2. Legg metadata-entry i GALLERY-listen under
  3. Bygg image på nytt
"""

import json
import pathlib

GALLERY_DIR = pathlib.Path(__file__).parent / "notebooks_gallery"

CATEGORIES = [
    ("cohort",     "Cohort-analyse"),
    ("timeseries", "Tidsserie"),
    ("governance", "Governance og PII"),
    ("sql",        "SQL og BI"),
    ("intro",      "Intro og oppslag"),
    ("ml",         "ML-eksperiment"),
]

DIFFICULTIES = ("begynner", "middels", "avansert")


GALLERY: list[dict] = [
    {
        "slug":          "folkeregister-cohort",
        "title":         "Folkeregister cohort-analyse",
        "description":   "Aldersskohorter beregnet fra fødselsdato. Viser fordeling per tiår og hvor mange som lever i Norge per kohort.",
        "category":      "cohort",
        "domain":        "folkeregister",
        "difficulty":    "begynner",
        "tags":          ["folkeregister", "cohort", "spark"],
        "preview": (
            "df = spark.read.format('delta').load('s3a://gold/folkeregister/cohort_demographics')\n"
            "df.groupBy('birth_decade').count().orderBy('birth_decade').show()"
        ),
        "filename":      "folkeregister_cohort.ipynb",
    },
    {
        "slug":          "folkeregister-migration",
        "title":         "Migrasjons-tidsserie per kommune",
        "description":   "Inn- og utflyttinger per kommune per måned. Identifiser kommuner med netto vekst eller nedgang.",
        "category":      "timeseries",
        "domain":        "folkeregister",
        "difficulty":    "middels",
        "tags":          ["folkeregister", "tidsserie", "geo"],
        "preview": (
            "df = spark.read.format('delta').load('s3a://silver/folkeregister/municipality_migration')\n"
            "df.filter(df.period >= '2026-01-01').orderBy('netto', ascending=False).show(20)"
        ),
        "filename":      "folkeregister_migration.ipynb",
    },
    {
        "slug":          "pii-audit",
        "title":         "PII-revisjonsmal (governance)",
        "description":   "Auditér hvilke kolonner som er merket som PII på tvers av alle dataprodukter. Gir en oversikt for GDPR-rapportering.",
        "category":      "governance",
        "domain":        "*",
        "difficulty":    "begynner",
        "tags":          ["governance", "pii", "gdpr"],
        "preview": (
            "import requests\n"
            "products = requests.get('http://dataportal.slettix-analytics.svc.cluster.local:8090/api/products').json()\n"
            "pii_cols = [(p['id'], c['name']) for p in products for c in p.get('schema', []) if c.get('pii')]"
        ),
        "filename":      "pii_audit.ipynb",
    },
    {
        "slug":          "sql-superset",
        "title":         "SQL-mal mot Gold-tabeller",
        "description":   "Koble til et dataprodukt via Superset SQL Lab og kjør grunnleggende aggregater. Utgangspunkt for dashboards.",
        "category":      "sql",
        "domain":        "*",
        "difficulty":    "begynner",
        "tags":          ["sql", "superset", "bi"],
        "preview": (
            "-- I Superset SQL Lab, mot 'slettix_gold'-database:\n"
            "SELECT domain, COUNT(*) AS rows FROM information_schema.tables\n"
            "WHERE table_schema='gold' GROUP BY domain ORDER BY rows DESC;"
        ),
        "filename":      "sql_superset.ipynb",
    },
    {
        "slug":          "delta-intro",
        "title":         "Delta Lake — første spørring",
        "description":   "Introduksjon til Delta Lake med PySpark: lese, filtrere, time-travel og inspisere skjema.",
        "category":      "intro",
        "domain":        "*",
        "difficulty":    "begynner",
        "tags":          ["delta", "spark", "intro"],
        "preview": (
            "df = spark.read.format('delta').load('s3a://silver/folkeregister/person_registry')\n"
            "df.printSchema()\n"
            "df.show(5)"
        ),
        "filename":      "delta_intro.ipynb",
    },
    {
        "slug":          "ml-experiment",
        "title":         "ML-eksperiment-mal",
        "description":   "Boilerplate for et ML-eksperiment: les Gold-tabell, splitt i train/test, tren en modell, logg resultatet.",
        "category":      "ml",
        "domain":        "*",
        "difficulty":    "avansert",
        "tags":          ["ml", "experiment", "scikit-learn"],
        "preview": (
            "from sklearn.linear_model import LogisticRegression\n"
            "from sklearn.model_selection import train_test_split\n"
            "df = spark.read.format('delta').load('s3a://gold/...').toPandas()\n"
            "X_train, X_test, y_train, y_test = train_test_split(df.drop('y', axis=1), df['y'])"
        ),
        "filename":      "ml_experiment.ipynb",
    },
]


def all_examples() -> list[dict]:
    return GALLERY


def by_category() -> list[tuple[str, str, list[dict]]]:
    grouped: dict[str, list[dict]] = {c: [] for c, _ in CATEGORIES}
    for ex in GALLERY:
        cat = ex.get("category", "")
        if cat in grouped:
            grouped[cat].append(ex)
    return [(slug, label, grouped[slug]) for slug, label in CATEGORIES if grouped[slug]]


def get_example(slug: str) -> dict | None:
    for ex in GALLERY:
        if ex["slug"] == slug:
            return ex
    return None


def load_notebook(slug: str) -> dict | None:
    """Les .ipynb-filen for et eksempel og returner som dict (Jupyter-format)."""
    ex = get_example(slug)
    if not ex:
        return None
    path = GALLERY_DIR / ex["filename"]
    if not path.exists():
        return None
    return json.loads(path.read_text())
