"""
Plattform-glossar (GUIDE-2 og GUIDE-7).

Sentralisert kilde for konseptforklaringer brukt både av:
  - `/glossary`-siden (full liste, søk, kategorier)
  - `help_tooltip(slug)`-makroen (kort forklaring + lenke til full term)

Hver term har:
  slug      — unik nøkkel (kebab-case), brukes som URL-anker (#slug)
  title     — visningsnavn
  category  — gruppering på glossar-siden
  short     — én-linjes forklaring brukt i tooltips
  long      — full markdown-forklaring brukt på glossar-siden
  related   — andre slugs med kontekst (vises som "Se også"-lenker)

Når nye termer dukker opp i UI, legg til her — ikke spred forklaringene.
"""

CATEGORIES = [
    ("arkitektur",  "Arkitektur og lagdeling"),
    ("data-mesh",   "Data Mesh og dataprodukter"),
    ("kvalitet",    "Kvalitet og kontrakter"),
    ("lineage",     "Lineage og avhengigheter"),
    ("storage",     "Lagring og tabeller"),
    ("personvern",  "Personvern og klassifisering"),
]


GLOSSARY: dict[str, dict] = {
    # ── Arkitektur og lagdeling ─────────────────────────────────────────
    "medallion": {
        "title":    "Medallion-arkitektur",
        "category": "arkitektur",
        "short":    "Tre-lagsmodell der data foredles fra rå (Bronze) via konformert (Silver) til ferdig modellert (Gold).",
        "long": (
            "Medallion-arkitekturen organiserer data i tre lag:\n\n"
            "- **Bronze** — rå inntak, ofte event-for-event fra Kafka. Bevarer alle felter "
            "som de kom inn slik at vi kan reprosessere ved behov.\n"
            "- **Silver** — rensede, konformerte og deduplikerte tabeller. Felles definisjoner "
            "på tvers av domener; her ligger dataprodukter som «person_registry».\n"
            "- **Gold** — sluttbruker-vennlige aggregater og analytiske produkter optimalisert "
            "for spørringer i Superset, Jupyter eller eksterne BI-verktøy.\n\n"
            "I plattformen tilsvarer hvert lag en MinIO-bucket med samme navn."
        ),
        "related": ["bronze", "silver", "gold", "delta-lake"],
    },
    "bronze": {
        "title":    "Bronze-laget",
        "category": "arkitektur",
        "short":    "Råinntak — events lagres uendret fra Kafka eller andre kilder.",
        "long": (
            "Bronze inneholder rå hendelsesdata uten transformasjon. Hver melding fra Kafka "
            "lagres som en rad i en Delta-tabell med `event_type`, `event_timestamp`, "
            "`payload_json` og metadata. IDP-er (streaming-jobber) skriver direkte hit.\n\n"
            "Hovedformål: kunne reprosessere Silver/Gold ved feil eller endringer."
        ),
        "related": ["medallion", "idp", "delta-lake"],
    },
    "silver": {
        "title":    "Silver-laget",
        "category": "arkitektur",
        "short":    "Konformerte og rensede tabeller — domeneprodukter som er ferdig modellert.",
        "long": (
            "Silver-laget inneholder dataprodukter som er rensede, validerte og konformerte. "
            "Personlige identifikatorer maskeres (`fnr_hash`), event-strømmer flates ut til "
            "rad-per-tilstand (SCD2), og enheter får konsistente IDer på tvers av kilder.\n\n"
            "Et Silver-produkt har eier, kontrakt, kvalitetskrav og publiseres som dataprodukt."
        ),
        "related": ["medallion", "scd2", "data-product", "schema-compatibility"],
    },
    "gold": {
        "title":    "Gold-laget",
        "category": "arkitektur",
        "short":    "Aggregerte, analytiske produkter for direkte konsum i BI og notebooks.",
        "long": (
            "Gold-tabeller er optimalisert for spørringer fra sluttbrukere. De inneholder "
            "ofte aggregater per domene (f.eks. innflyttinger per kommune per måned), "
            "denormaliserte joins, eller spesifikke views skreddersydd for et dashboard.\n\n"
            "Gold er konsumstedet for Superset, Jupyter og eksterne klienter."
        ),
        "related": ["medallion", "data-product"],
    },

    # ── Data Mesh og dataprodukter ──────────────────────────────────────
    "data-mesh": {
        "title":    "Data Mesh",
        "category": "data-mesh",
        "short":    "Distribuert datafilosofi der domener eier sine data som produkter.",
        "long": (
            "Data Mesh er en organisasjonsfilosofi der hvert forretningsdomene "
            "(folkeregister, HR, etc.) eier sine egne data og publiserer dem som "
            "**dataprodukter** med tydelig kontrakt, eierskap og kvalitetskrav. "
            "Sentralt plattform-team (oss) leverer infrastrukturen, mens domenene leverer dataene.\n\n"
            "Konsekvenser i plattformen:\n"
            "- Hvert produkt har en `owner` og et domene\n"
            "- Tilgangsstyring kan begrenses per domene (`restricted` access)\n"
            "- Skjema-endringer går via kontrakt-kompatibilitet"
        ),
        "related": ["data-product", "data-contract", "schema-compatibility"],
    },
    "data-product": {
        "title":    "Dataprodukt",
        "category": "data-mesh",
        "short":    "Et publisert datasett med eier, kontrakt, kvalitetskrav og dokumentasjon.",
        "long": (
            "Et dataprodukt i plattformen er en Delta-tabell registrert i `gold/data_products` "
            "med et manifest som spesifiserer:\n\n"
            "- Eier (person eller team)\n"
            "- Domene\n"
            "- Beskrivelse og bruksformål\n"
            "- Skjema (kolonnenavn, typer, PII-flag)\n"
            "- Kontrakt (skjema-kompatibilitet, SLA)\n"
            "- Tilgangsnivå (public eller restricted)\n"
            "- Lineage (avhengigheter til upstream-produkter)\n\n"
            "Publiseres via `/publish` eller fra Jupyter-kode."
        ),
        "related": ["data-mesh", "data-contract", "lineage", "manifest"],
    },
    "data-contract": {
        "title":    "Datakontrakt",
        "category": "data-mesh",
        "short":    "Avtalen mellom produsent og konsument — skjema, SLA, kvalitet, kompatibilitet.",
        "long": (
            "En datakontrakt er en formell avtale mellom et dataproduktteam og dets konsumenter. "
            "Den spesifiserer hva konsumenter kan stole på:\n\n"
            "- **Skjema-kompatibilitet** (BACKWARD/FORWARD/FULL/NONE)\n"
            "- **SLA** (freshness, completeness)\n"
            "- **Garanterte kolonner** som ikke fjernes uten varsel\n"
            "- **Datakvalitetsregler** som valideres ved hver kjøring\n\n"
            "Endringer som bryter kontrakten varsler alle abonnenter."
        ),
        "related": ["schema-compatibility", "sla", "data-product"],
    },

    # ── Kvalitet og kontrakter ──────────────────────────────────────────
    "schema-compatibility": {
        "title":    "Skjema-kompatibilitet",
        "category": "kvalitet",
        "short":    "Regel som styrer hvilke skjema-endringer som tillates uten å bryke konsumentene.",
        "long": (
            "Fire kompatibilitetsmodi:\n\n"
            "- **BACKWARD** — nye konsumenter kan lese gamle data. Tillatt: legge til nullable kolonner.\n"
            "- **FORWARD** — gamle konsumenter kan lese nye data. Tillatt: fjerne nullable kolonner.\n"
            "- **FULL** — både og. Tillatt: kun trygge endringer (legge til/fjerne nullable felt).\n"
            "- **NONE** — ingen kompatibilitetsgaranti. Vil bryte konsumenter ved endringer.\n\n"
            "Når et nytt manifest publiseres, sjekkes det mot eksisterende kontrakt før det aksepteres."
        ),
        "related": ["data-contract"],
    },
    "sla": {
        "title":    "SLA og freshness",
        "category": "kvalitet",
        "short":    "Avtalt maks-tid mellom oppdateringer. Brytes hvis pipelinen henger.",
        "long": (
            "SLA-regelen `quality_sla.freshness_hours` setter maksimal tid mellom hver "
            "oppdatering av tabellen. SLA-monitoren `02_sla_monitor` sjekker hvert 30. minutt "
            "om siste Delta-commit er innenfor terskelen.\n\n"
            "Brudd vises som rødt merke på katalog- og produktdetaljside, og logges i "
            "30-dagers historikk for compliance-rapportering."
        ),
        "related": ["data-contract", "quality-rules"],
    },
    "quality-rules": {
        "title":    "Datakvalitetsregler",
        "category": "kvalitet",
        "short":    "Regler kjørt etter hver pipeline-kjøring (f.eks. ingen null i nøkkelkolonne).",
        "long": (
            "Etter at en Silver-jobb har skrevet, kjøres en valideringsjobb som sjekker "
            "`expect_*`-regler mot tabellen:\n\n"
            "- `expect_column_values_to_not_be_null`\n"
            "- `expect_column_to_exist`\n"
            "- `expect_table_row_count_to_be_between`\n\n"
            "Resultatet `score_pct` vises som grønt (100%) eller rødt (< 100%) merke. "
            "Detaljer per regel ligger i historikken på produktdetaljsiden."
        ),
        "related": ["data-contract", "sla"],
    },

    # ── Lineage og avhengigheter ────────────────────────────────────────
    "lineage": {
        "title":    "Lineage",
        "category": "lineage",
        "short":    "Sporbarhet mellom dataprodukter — hva er bygget av hva.",
        "long": (
            "Lineage er den eksplisitte avhengighetsgrafen mellom dataprodukter. To nivåer:\n\n"
            "- **Tabell-nivå** — produkt B er bygget fra produkt A (via `source_products` i manifest)\n"
            "- **Kolonne-nivå** — kolonne X i B er avledet av kolonner Y og Z i A "
            "(via `column_lineage` i manifest)\n\n"
            "Lineage-grafen vises som mermaid-diagram på produktdetaljsiden og brukes av "
            "konsekvensanalyse: hvis vi endrer A, hvilke produkter må re-bygges?"
        ),
        "related": ["data-product", "manifest"],
    },

    # ── Lagring og tabeller ─────────────────────────────────────────────
    "delta-lake": {
        "title":    "Delta Lake",
        "category": "storage",
        "short":    "Transaksjonelt tabellformat over parquet — gir ACID, time travel og schema-evolution.",
        "long": (
            "Delta Lake er lagringsformatet for alle tabeller på plattformen. Det tilbyr:\n\n"
            "- **ACID-transaksjoner** — trygge skrivinger fra flere jobber samtidig\n"
            "- **Time travel** — se tabellen som den var ved et tidspunkt (`VERSION AS OF`)\n"
            "- **Schema-evolution** — legge til kolonner uten å skrive om data\n"
            "- **Effektive merges** — for SCD2-mønstre\n\n"
            "Hver tabell har en `_delta_log/`-mappe ved siden av parquet-filene som inneholder "
            "transaksjonsloggen."
        ),
        "related": ["medallion", "scd2"],
    },
    "scd2": {
        "title":    "SCD Type 2",
        "category": "storage",
        "short":    "Lagringsmønster der hver tilstandsendring blir en ny rad — bevarer historikk.",
        "long": (
            "Slowly Changing Dimensions Type 2: når en attributt på en entitet endres "
            "(f.eks. adresse), legges en ny rad til istedenfor å overskrive. "
            "Personregisteret bruker dette for adresse, sivilstatus og vitalstatus.\n\n"
            "Hver rad har gyldighetsperiode (`effective_from`, `effective_to`) eller "
            "is_current-flagg. For å se gjeldende tilstand: filter på `is_current = true`."
        ),
        "related": ["delta-lake"],
    },
    "manifest": {
        "title":    "Manifest",
        "category": "storage",
        "short":    "JSON-spesifikasjonen som beskriver et dataprodukt — eier, skjema, SLA, lineage.",
        "long": (
            "Et manifest er JSON-en som registreres i `gold/data_products`-Delta-tabellen "
            "via `register()` (fra Jupyter eller portalens `/publish`-skjema). Inneholder "
            "alle felter som vises på produktdetaljsiden.\n\n"
            "Hver `register()`-kall lager en ny versjon (append-only); historikk vises som "
            "diff på produktdetaljsiden."
        ),
        "related": ["data-product", "schema-compatibility"],
    },

    # ── Streaming og pipelines ──────────────────────────────────────────
    "idp": {
        "title":    "IDP — Inbound Data Pipeline",
        "category": "arkitektur",
        "short":    "Streaming-jobb som leser Kafka og skriver til Bronze i sanntid.",
        "long": (
            "En IDP er en Spark Structured Streaming-jobb som kjører som SparkApplication-CRD "
            "i Kubernetes. Den leser én eller flere Kafka-topics og skriver hver melding til "
            "en Delta-tabell i Bronze-bucketen.\n\n"
            "Hver IDP er knyttet til ett eller flere dataprodukter via "
            "`metadata.annotations.slettix.io/product-id`. Status (`RUNNING`, `FAILED`, uptime) "
            "vises på produktdetaljsiden under «IDP-status»."
        ),
        "related": ["bronze", "streaming-vs-batch"],
    },
    "streaming-vs-batch": {
        "title":    "Streaming vs batch",
        "category": "arkitektur",
        "short":    "Streaming = kontinuerlig, lav latency. Batch = kjøres på timeplan, høyere gjennomstrøming.",
        "long": (
            "**Streaming** (IDP-er): Kafka → Bronze, kontinuerlig, ~sekunder latency. "
            "Brukes for hendelsesdata som personregisteringer.\n\n"
            "**Batch** (Airflow-DAG-er): Bronze → Silver → Gold, kjøres på timeplan "
            "(f.eks. nattlig). Brukes for transformasjoner som krever full tabell-pass "
            "(joins, aggregater, SCD2-merges).\n\n"
            "Plattformen kombinerer begge: streaming inn til Bronze, batch til Silver/Gold."
        ),
        "related": ["idp", "bronze", "silver"],
    },

    # ── Personvern ──────────────────────────────────────────────────────
    "pii": {
        "title":    "PII — personidentifiserende informasjon",
        "category": "personvern",
        "short":    "Data som direkte eller indirekte kan identifisere en person.",
        "long": (
            "PII (Personally Identifiable Information) markeres på kolonnenivå i manifestet "
            "via `pii: true` og en `sensitivity`-grad (low/medium/high). Plattformen "
            "håndhever:\n\n"
            "- Maskering av PII-kolonner for brukere uten PII-tilgang\n"
            "- `restricted` access på produktnivå krever eksplisitt godkjenning\n"
            "- Dataportalen viser PII-flagg ved siden av kolonnenavn i schema-tabellen\n"
            "- GDPR-datakart (`/governance`) viser hvor PII finnes"
        ),
        "related": ["data-classification"],
    },
    "data-classification": {
        "title":    "Dataklassifisering og sensitivity",
        "category": "personvern",
        "short":    "Gradering av hvor sensitiv en kolonne er — påvirker tilgang og maskering.",
        "long": (
            "Hver kolonne kan ha `sensitivity`-felt:\n\n"
            "- **low** — generell informasjon, ingen restriksjoner\n"
            "- **medium** — krever brukerinnlogging\n"
            "- **high** — kan kreve eksplisitt PII-tilgangsrolle (f.eks. fnr_hash, full_name)\n\n"
            "Klassifiseringen brukes til å automatisk maskere kolonner i API-svar og UI."
        ),
        "related": ["pii"],
    },
}


def get_term(slug: str) -> dict | None:
    return GLOSSARY.get(slug)


def all_terms() -> list[dict]:
    """Returnerer alle termer som flat liste, sortert alfabetisk."""
    return sorted(
        ({"slug": s, **t} for s, t in GLOSSARY.items()),
        key=lambda t: t["title"].lower(),
    )


def by_category() -> list[tuple[str, str, list[dict]]]:
    """Returnerer (slug, label, terms) per kategori i definert rekkefølge."""
    grouped: dict[str, list[dict]] = {c: [] for c, _ in CATEGORIES}
    for slug, term in GLOSSARY.items():
        cat = term.get("category", "")
        if cat in grouped:
            grouped[cat].append({"slug": slug, **term})
    for cat in grouped:
        grouped[cat].sort(key=lambda t: t["title"].lower())
    return [(slug, label, grouped[slug]) for slug, label in CATEGORIES if grouped[slug]]
