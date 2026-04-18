# Slettix Analytics — Platform Architecture

**Version:** 1.0  
**Date:** April 2026

---

## Table of Contents

1. [Platform Overview](#1-platform-overview)
2. [Guiding Principles](#2-guiding-principles)
3. [Technology Stack](#3-technology-stack)
4. [Infrastructure and Deployment](#4-infrastructure-and-deployment)
5. [Data Architecture — Medallion Layers](#5-data-architecture--medallion-layers)
6. [Data Mesh — Domain Ownership](#6-data-mesh--domain-ownership)
7. [Data Ingestion — Input Data Products](#7-data-ingestion--input-data-products)
8. [Data Transformation — Silver and Gold](#8-data-transformation--silver-and-gold)
9. [Data Portal and Product Registry](#9-data-portal-and-product-registry)
10. [Data Quality and Observability](#10-data-quality-and-observability)
11. [Lineage and Governance](#11-lineage-and-governance)
12. [Self-Service — Pipeline Builder](#12-self-service--pipeline-builder)
13. [Analytics and Exploration](#13-analytics-and-exploration)
14. [Authentication and Access Control](#14-authentication-and-access-control)
15. [Component Dependency Graph](#15-component-dependency-graph)

---

## 1. Platform Overview

Slettix Analytics is a **data mesh platform** built on open-source components and deployed on Kubernetes. It enables distributed domain teams to publish, discover, and consume data products independently, while a central platform team maintains shared infrastructure and governance standards.

The platform's primary use case is the Norwegian National Registry (*Folkeregisteret*) — simulated data covering population statistics, migration flows, civil status, household structures, and cohort demographics — alongside HR domain data. These serve as reference implementations that domain teams can replicate for their own data products.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SLETTIX ANALYTICS PLATFORM                          │
│                                                                             │
│  PRODUCE                    STORE & TRANSFORM              CONSUME          │
│  ─────────                  ─────────────────              ───────          │
│  Kafka Topics               Bronze (raw events)            Data Portal      │
│  CSV / API                  Silver (curated)               Jupyter          │
│  Seed generators            Gold (aggregated)              Superset         │
│                             Delta Lake on MinIO            API / SDK        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Guiding Principles

| Principle | Implementation |
|-----------|----------------|
| **Domain ownership** | Each domain owns its data products from source to gold |
| **Data as a product** | Every dataset is a versioned, documented product with SLA and schema contracts |
| **Self-serve infrastructure** | Domain teams deploy pipelines and IDPs via the Portal UI — no platform team involvement required |
| **Federated governance** | PII classification, retention policies, and schema contracts are enforced automatically |
| **Interoperability** | All products expose the same API surface and Delta Lake format |
| **Observability by default** | Quality history, SLA compliance, anomaly detection, and incident management are built-in |

---

## 3. Technology Stack

### Core Data Processing

| Component | Version | Role |
|-----------|---------|------|
| **Apache Spark** | 3.5.8 | Batch and streaming data processing |
| **Delta Lake** | 3.2.0 | ACID table format on object storage; time travel; schema enforcement |
| **Apache Kafka** | *(external)* | Event streaming backbone; CloudEvent envelope standard |
| **MinIO** | Latest | S3-compatible object storage for Bronze/Silver/Gold/checkpoints |
| **delta-rs** | *(latest)* | Python-native Delta Lake reads without a Spark session (used in Airflow tasks and portal) |
| **DuckDB** | *(latest)* | In-notebook analytical queries via `delta_scan()` over MinIO |

### Orchestration

| Component | Version | Role |
|-----------|---------|------|
| **Apache Airflow** | 2.9.2 | Pipeline scheduling, monitoring, and DAG management |
| **Spark Operator** | *(k8s)* | Manages SparkApplication lifecycle in Kubernetes |
| **SparkKubernetesOperator** | *(Airflow)* | Submits batch Spark jobs as transient k8s pods from Airflow DAGs |

### Storage and Serving

| Component | Version | Role |
|-----------|---------|------|
| **MinIO** | *(latest)* | Object storage (buckets: `raw`, `bronze`, `silver`, `gold`, `checkpoints`) |
| **PostgreSQL** | 16 | Airflow metadata database |
| **SQLite** | *(embedded)* | Data portal: user accounts, incidents, subscriptions, access requests |

### Portal and APIs

| Component | Version | Role |
|-----------|---------|------|
| **FastAPI** | *(latest)* | Data portal REST API and HTML server-side rendering |
| **Jinja2** | *(latest)* | HTML template engine for portal UI |
| **httpx** | *(latest)* | Async HTTP client for Airflow API calls from portal |
| **boto3** | *(latest)* | S3/MinIO client for reading quality results, SLA history, etc. |

### Analytics and Visualization

| Component | Version | Role |
|-----------|---------|------|
| **JupyterLab** | 3.5.8 | Interactive analysis notebooks; Spark kernel |
| **Apache Superset** | 4.1.1 | BI dashboards connected to Gold-layer Delta tables via DuckDB |
| **Matplotlib / Plotly** | *(latest)* | Notebook charting |

### Governance and Quality

| Component | Version | Role |
|-----------|---------|------|
| **Great Expectations** | *(latest)* | Data expectation definitions; validation results in JSON |
| **Kubernetes Python client** | *(latest)* | Dynamic SparkApplication creation from pipeline-builder DAGs |

### Infrastructure

| Component | Role |
|-----------|------|
| **Kubernetes (Docker Desktop)** | Multi-node container orchestration (3 nodes) |
| **Helm** | Package management for Airflow deployment |
| **Docker** | Container image builds for Spark, Airflow, Jupyter, Portal, Superset |

---

## 4. Infrastructure and Deployment

The platform runs on a **3-node Kubernetes cluster** provided by Docker Desktop (kind-based), with the following node layout:

```
docker-desktop          — control plane + workloads
desktop-worker          — Spark executor pods
desktop-worker2         — Spark executor pods
```

All services run in the `slettix-analytics` namespace.

### Kubernetes Workloads

| Workload | Type | Description |
|----------|------|-------------|
| `dataportal` | Deployment | FastAPI portal, mounts jobs/ and dags/ via ConfigMaps |
| `airflow-webserver` | Deployment | Airflow UI and REST API |
| `airflow-scheduler` | Deployment | DAG scheduling engine |
| `airflow-worker` | StatefulSet | Celery task executor |
| `minio` | Deployment | S3-compatible object store |
| `postgres` | Deployment | Airflow metadata DB |
| `superset` | Deployment | BI dashboard server |
| `jupyter` | Deployment | JupyterLab with Spark kernel |
| `kafka` | *(external namespace: customermaster)* | Kafka cluster for event streaming |
| `*-idp` | SparkApplication | Long-running streaming IDPs (managed by Spark Operator) |

### Image Distribution

Python jobs (`jobs/*.py`) are distributed to Airflow and the portal via **ConfigMaps** mounted as files. This eliminates the need to rebuild Docker images when adding or modifying batch job code. Spark jobs used for streaming IDPs are baked into the Spark Docker image at build time (`COPY jobs/ /opt/spark/jobs/`).

### Service Discovery

All inter-service communication uses Kubernetes internal DNS:

```
http://minio.slettix-analytics.svc.cluster.local:9000
http://dataportal.slettix-analytics.svc.cluster.local:8090
http://airflow-webserver.slettix-analytics.svc.cluster.local:8080
http://kafka.customermaster.svc.cluster.local:9092
```

External access is provided via `LoadBalancer` services and local port-forwards for development.

---

## 5. Data Architecture — Medallion Layers

The platform uses a **Medallion (multi-hop) architecture** with four distinct storage layers, all persisted as Delta tables in MinIO.

```
Kafka / CSV / API
       │
       ▼
┌─────────────┐
│     RAW     │  s3://raw/           Original files, no transformation
└─────────────┘
       │
       ▼
┌─────────────┐
│   BRONZE    │  s3://bronze/        Parsed CloudEvents; PII masked; schema enforced
│             │  partitioned by      Full event history; append-only
│             │  event_type/date     Includes _processed_at, _source_topics
└─────────────┘
       │
       ▼
┌─────────────┐
│   SILVER    │  s3://silver/        Curated, deduplicated, SCD Type 2
│             │                     Domain keys (person_id, not fnr)
│             │                     Validated by Great Expectations
└─────────────┘
       │
       ▼
┌─────────────┐
│    GOLD     │  s3://gold/          Aggregated, analytics-ready
│             │                     Dimensional models; time series
│             │                     Accessible via DuckDB / Superset
└─────────────┘
```

### Layer Contracts

| Layer | Mutability | Schema Evolution | PII Policy |
|-------|-----------|-----------------|------------|
| Bronze | Append-only | `autoMerge` enabled | PII masked/hashed at write time |
| Silver | Merge (SCD2) | Schema validated | No raw PII; pseudonymized keys |
| Gold | Overwrite/append | Stable contracts | Aggregated; no individual-level PII |

### Delta Lake Features in Use

- **Time travel** — version history queryable via `DeltaTable.history()`
- **Schema enforcement** — schemas defined in SparkSession or expectation suites
- **Schema autoMerge** — IDP streaming jobs add new columns without restart (`spark.databricks.delta.schema.autoMerge.enabled = true`)
- **ACID transactions** — concurrent writes safe across Airflow tasks
- **Checkpoint-based streaming** — exactly-once delivery for Kafka → Bronze

### Storage Layout

```
s3://bronze/
  folkeregister/
    person_events/      ← Delta table, partitioned by event_type/event_date
    family_events/
s3://silver/
  folkeregister/
    person_registry/    ← SCD Type 2 accumulation; one row per person (latest state)
    family_relations/
    residence_history/
  hr/
    employees/
s3://gold/
  folkeregister/
    population_stats/
    migration_flows/
    marital_status_trends/
    household_structure/
    cohort_demographics/
  hr/
    department_stats/
  data_products/        ← Product registry (Delta table)
  quality_results/      ← GE validation JSON per product
  sla_results/          ← SLA check JSON per product (latest + history)
  pipeline_configs/     ← Saved Pipeline Builder configurations
s3://checkpoints/       ← Spark Structured Streaming checkpoints
```

---

## 6. Data Mesh — Domain Ownership

The platform implements the four Data Mesh principles as follows:

### Domain-Oriented Ownership

Products are organized by domain (`folkeregister`, `hr`, `analytics`). Each product manifest declares its owner team. The portal enforces access control at the domain level — team members with domain privileges can publish, update, and view restricted products within their domain.

### Data as a Product

Every dataset is registered as a **data product** with a standardized manifest (`conf/products/<id>.json`) containing:

```json
{
  "id":           "folkeregister.person_events",
  "name":         "...",
  "domain":       "folkeregister",
  "owner":        "folkeregister-team",
  "version":      "1.0.0",
  "source_path":  "s3://bronze/folkeregister/person_events",
  "format":       "delta",
  "product_type": "source",
  "access":       "restricted",
  "quality_sla":  { "freshness_hours": 1 },
  "contract":     { "slo": {...}, "schema_compatibility": "BACKWARD" },
  "schema":       [ { "name": "event_id", "pii": false, ... } ],
  "lineage":      { "column_lineage": [...] }
}
```

The manifest is the **single source of truth** for schema documentation, SLA contracts, PII classification, and lineage. It is version-controlled in git and loaded into a Delta Lake registry table at pipeline runtime.

### Self-Serve Data Infrastructure

Domain teams interact with the platform entirely through the **Data Portal UI** or REST API:

- **Discover** products via catalog with full-text search
- **Explore** schema, lineage, and quality history without contacting the platform team
- **Publish** new data products via a form (analytical products) or the Pipeline Builder (pipelines and IDPs)
- **Subscribe** to change notifications
- **Request access** to restricted products with approval workflow

No Kubernetes access, no git commits, and no image rebuilds are required for the majority of domain team workflows.

### Federated Computational Governance

Governance rules are encoded as platform behaviour, not manual processes:

| Governance concern | Mechanism |
|--------------------|-----------|
| PII masking | SHA-256 hashing in Bronze IDP; Silver jobs never receive raw PII |
| Retention | `governance_retention` DAG auto-deletes Bronze partitions older than `retention_days` |
| Schema contracts | `BACKWARD`/`FORWARD` compatibility enforced in manifest; validated in quality checks |
| Access control | `restricted` products require explicit access grants; all API calls check token/API key |
| GDPR data map | Portal `/governance` page aggregates all PII columns across all products |

---

## 7. Data Ingestion — Input Data Products

### Streaming IDPs (Kafka → Bronze)

The primary ingestion pattern is **Structured Streaming from Apache Kafka**. Events follow the **CloudEvent** envelope standard (camelCase JSON).

```
Kafka Topic(s)
     │  CloudEvent JSON
     ▼
kafka_to_bronze.py (Spark Structured Streaming)
     │  - Parse CloudEvent envelope (eventId, eventType, occurredAt, source)
     │  - Store payload as payload_json (raw JSON — generic)
     │  - Add _processed_at, kafka_topic, kafka_partition, kafka_offset
     │  - Partition by event_type, event_date
     ▼
s3://bronze/<domain>/<product>/   (Delta table, append-only)
```

**Domain-specific IDPs** (e.g., `folkeregister_person_idp.py`) extend the generic pattern with domain knowledge: they unpack specific payload fields, apply PII hashing (`fnr → fnr_hash`), and map CloudEvent fields to canonical Bronze column names.

**Generic IDPs** (created via Pipeline Builder) use `kafka_to_bronze.py`, which stores `payload_json` as a raw JSON string. Silver jobs are responsible for domain-specific transformation.

Streaming jobs run as **SparkApplication** resources managed by the Spark Operator with `restartPolicy: Always`, ensuring automatic restart on failure.

### Batch Ingestion (CSV / API)

Batch sources are ingested via Airflow-scheduled SparkSubmitOperator or PythonOperator tasks:

- **CSV**: `ingest_to_bronze.py` reads from `s3://raw/`, applies type coercion, and writes to `s3://bronze/` as Delta
- **API**: PythonOperator fetches JSON via HTTP, converts to pandas/PyArrow, writes via delta-rs

### Seed Data

The `folkeregister_seed` DAG generates synthetic population data (800,000 persons across 356 municipalities) using configurable Faker-based generators. This serves as a reference dataset for development and testing.

---

## 8. Data Transformation — Silver and Gold

### Silver Layer — Curation and SCD Type 2

Silver jobs read from Bronze and apply:

1. **Deduplication** — remove duplicate events by `event_id`
2. **Type casting** — parse date/timestamp strings into native types
3. **SCD Type 2 accumulation** — build current state from event stream:
   ```
   LAST(col IGNORE NULLS) OVER (PARTITION BY person_id ORDER BY event_ts)
   ```
4. **Validation** — Great Expectations suite run after write; results stored in MinIO

The Silver `person_registry` table is the authoritative current-state store for persons, populated by accumulating `citizen.created`, `event.birth`, `citizen.died`, and `event.relocation` events.

### Gold Layer — Aggregation and Analytics

Gold jobs aggregate Silver data into analytics-ready tables:

| Gold product | Description | Source |
|-------------|-------------|--------|
| `population_stats` | Annual population totals, births, deaths per municipality | Silver `person_registry` |
| `migration_flows` | Pairwise migration counts between municipalities | Silver `residence_history` |
| `marital_status_trends` | Civil status distribution by age group and year | Silver `person_registry` |
| `household_structure` | Household type and size categories | Silver `family_relations` |
| `cohort_demographics` | Birth cohort survival rates, marriage rates, mobility | Silver `person_registry` |
| `hr.department_stats` | Headcount, salary statistics, tenure per department | Silver `hr.employees` |

Gold tables are written with `overwrite` mode on each DAG run (idempotent, no history needed at this layer).

### DAG Pipeline Structure

```
folkeregister_seed  ──►  folkeregister_idp (streaming, always-on)
                                │
                                ▼
                         folkeregister_silver  (daily)
                         ├── build_person_registry
                         ├── build_family_relations
                         └── build_residence_history
                                │
                                ▼
                         folkeregister_gold  (daily)
                         ├── population_stats
                         ├── migration_flows
                         ├── marital_status_trends
                         ├── household_structure
                         └── cohort_demographics

01_slettix_pipeline ──►  ingest → bronze → silver → validate → gold → anomaly → register

02_sla_monitor      ──►  freshness check every 30 min → sla_results/{id}/latest.json
                                                       → sla_results/{id}/history/

governance_retention ──► delete Bronze partitions older than retention_days (daily)
```

---

## 9. Data Portal and Product Registry

The **Data Portal** (`dataportal/`) is the central interface for the platform. It is a FastAPI application serving both a REST API and server-side rendered HTML pages.

### Product Registry

The registry is a **Delta Lake table** at `s3://gold/data_products/`. Each row represents one published version of a product manifest. `list_all()` returns the most recent version per product by deduplicating on `registered_at DESC`.

Registration is triggered automatically by the `register_dag_link` / `register_product` tasks at the end of each pipeline DAG run — no manual step required.

### Portal API Surface

```
GET  /api/products                    List all accessible products
GET  /api/products/{id}               Full manifest with schema, SLA, pipeline status
GET  /api/products/{id}/schema        Delta table schema (merged with manifest)
GET  /api/products/{id}/quality       Latest Great Expectations result
GET  /api/products/{id}/quality/history  Quality time series (last 30 runs)
GET  /api/products/{id}/sla           Current SLA compliance status
GET  /api/products/{id}/sla/history   SLA compliance % (30 days) + MTTR
GET  /api/products/{id}/anomalies     Latest anomaly detection result
GET  /api/products/{id}/incidents     Incident list for product
GET  /api/products/{id}/lineage       Column-level + product-level lineage graph
GET  /api/products/{id}/versions      Version history with diffs
GET  /api/products/{id}/related       Recommended related products (scored)

POST /api/pipelines                   Deploy a generated DAG
POST /api/pipelines/preview           Preview generated DAG code
POST /api/nl2sql                      Natural language → SQL (Claude API)
GET  /api/governance/pii-map          GDPR map: all PII columns across all products
```

### Schema Merging

The portal merges the **physical Delta table schema** (authoritative for existing columns) with the **manifest schema** (authoritative for metadata: PII flags, descriptions, sensitivity). Columns documented in the manifest but not yet present in the physical table are shown with a `pending` badge — this bridges the gap between documentation and implementation for teams documenting schema before pipeline execution.

### Consumer SDK — `slettix_client.py`

A Python client library is distributed to Jupyter via ConfigMap. It provides typed functions (`get_manifest`, `get_quality`, `get_sla`, `publish_analytical`) that wrap the portal API with automatic API key injection and graceful 404 handling.

---

## 10. Data Quality and Observability

### Quality Validation (Great Expectations)

Each Silver and Gold pipeline run invokes a GE validation suite. Results are written to:

```
s3://gold/quality_results/{product_id}/latest.json
s3://gold/quality_results/{product_id}/history/{YYYYMMDDTHHmmss}.json
s3://gold/quality_results/{product_id}/anomalies/latest.json
```

The portal product page renders a Chart.js time series of `score_pct` over the last 30 runs, with a per-expectation breakdown table.

### SLA Monitoring

The `02_sla_monitor` DAG runs every 30 minutes and checks freshness for every product with a `quality_sla.freshness_hours` definition. It reads the Delta table's last commit timestamp via delta-rs (no Spark required) and compares it against the SLA threshold.

Results are written to:
```
s3://gold/sla_results/{product_id}/latest.json
s3://gold/sla_results/{product_id}/history/{YYYYMMDDTHHmmss}.json
```

The portal computes **SLA compliance %** (last 30 days) from the history files and displays it on the product page with a color-coded indicator (≥99% green, ≥90% yellow, <90% red).

### Anomaly Detection

The `detect_anomaly` task in the HR pipeline reads the last 30 quality history files and computes statistical baselines (mean, standard deviation) for `score_pct`, `row_count`, and `null_rate`. If current values deviate by more than 2σ, an anomaly record is written and an incident is automatically created.

### Incident Management

Incidents are stored in a SQLite table in the portal (`auth.db`) with severity, status, and timestamps. When an incident transitions to `resolved`, `resolved_at` is recorded. The portal computes **MTTR** (Mean Time To Resolve) as the mean of `(resolved_at − created_at)` for all resolved incidents on a product.

### Platform-Wide Observability Dashboard

The `/observability` page aggregates health signals across all products:
- Per-product quality score, SLA status, and open incidents
- Platform totals: compliant products, active incidents, products without recent quality runs
- Timeline of recent quality runs and SLA breaches

---

## 11. Lineage and Governance

### Product-Level Lineage

The manifest `source_products` field declares upstream data product dependencies. The portal renders a **Mermaid flowchart** showing the full lineage graph (upstream + downstream) with medallion layer labels.

```json
"source_products": ["folkeregister.person_events", "folkeregister.family_events"]
```

### Column-Level Lineage

Column lineage is declared explicitly in the manifest:

```json
"column_lineage": [
  {
    "column": "street_address",
    "sources": [{ "column": "payload.address" }],
    "transformation": "payload.address — street address, set on citizen.created (v2)"
  }
]
```

This is **manually maintained** by the team responsible for the transformation. The portal renders it as a column-by-column table on the product page.

### GDPR Governance

The `governance` page provides a cross-platform PII map showing every column marked `"pii": true` across all products, with sensitivity level, owner, and data product link. This enables data protection officers to audit which data products contain personal data without inspecting code.

The `governance_retention` DAG enforces **data retention policies** declared in the manifest (`retention_days`). It deletes Bronze Delta partitions (by `event_date`) older than the declared threshold using `DeltaTable.delete()`, and logs deletions to an audit trail.

### Schema Contracts

Each product can declare a contract with:
- **SLO** (Service Level Objective): `freshness_hours`, `completeness_pct`
- **Schema compatibility**: `BACKWARD`, `FORWARD`, or `FULL`
- **Guaranteed columns**: columns that consumers can rely on never being removed

The portal `/domain/{domain}/contracts` page shows a traffic-light summary of contract compliance across all products in a domain, exportable as CSV.

---

## 12. Self-Service — Pipeline Builder

The Pipeline Builder (`/pipeline-builder`) enables domain teams to create and deploy data pipelines without writing code or interacting with the platform team.

### Available Templates

| Template | Type | Description |
|----------|------|-------------|
| `csv_ingest` | Batch | CSV from MinIO raw → Bronze Delta; SparkSubmitOperator |
| `api_ingest` | Batch | REST API → Bronze Delta; PythonOperator |
| `silver_transform` | Batch | Bronze → Silver with config-driven transformation |
| `gold_aggregate` | Batch | Silver → Gold with config-driven aggregation |
| `scheduled_notebook` | Batch | Run a Jupyter notebook on a schedule via Papermill |
| `kafka_idp` | **Streaming** | Kafka → Bronze: creates a persistent SparkApplication and registers the product |

### Kafka IDP Workflow

The `kafka_idp` template is the platform's primary self-service path for domain teams who want to publish source data from Kafka:

```
1. Team fills out form in Pipeline Builder
   (topics, target path, consumer group, product ID, domain, owner)

2. Portal generates Airflow DAG with two tasks:
   deploy_streaming_job  → creates SparkApplication via kubernetes.client
   register_product      → registers product manifest in Delta Registry

3. DAG is written to /opt/airflow/dags/ (Airflow picks it up automatically)

4. Team triggers the DAG once in Airflow UI

5. Spark Operator creates the streaming pod
   └── runs kafka_to_bronze.py with parameterized topics/paths
   └── restartPolicy: Always (survives pod/node failures)

6. Product appears in Data Portal immediately
```

The generated SparkApplication uses the generic `kafka_to_bronze.py` job (already in the Spark image), which stores payload as `payload_json`. The team writes a Silver job to extract domain-specific fields — this separation keeps the IDP generic and stable even as the event schema evolves.

### Deployment Mechanics

Generated DAG files are written directly to the shared DAGs volume (`/opt/airflow/dags/`). Airflow's DAG file processor picks them up within 30–60 seconds. Pipeline configurations are also versioned in MinIO at `s3://gold/pipeline_configs/{dag_id}/`.

---

## 13. Analytics and Exploration

### Jupyter Notebooks

JupyterLab provides an interactive analysis environment with:
- **PySpark kernel** — full Spark session connected to MinIO
- **DuckDB** — lightweight analytical queries using `delta_scan('s3://...')` (no Spark required)
- **slettix_client.py** — portal SDK for manifest/quality/SLA metadata
- **Plotly + Matplotlib** — visualization

Domain-specific notebooks illustrate reference patterns:

| Notebook | Domain | Description |
|----------|--------|-------------|
| `folkeregister_befolkningsvekst.ipynb` | National Registry | Population growth, births vs. deaths, municipality trends |
| `folkeregister_migrasjon.ipynb` | National Registry | Migration flows, net migration, Sankey diagram |
| `folkeregister_demografiprofil.ipynb` | National Registry | Cohort survival, marriage/divorce rates, geographic mobility |
| `folkeregister_familie_sosiologi.ipynb` | National Registry | Civil status trends, household structures |
| `employee_satisfaction_analysis.ipynb` | HR | Satisfaction scores, correlation analysis |

### Apache Superset

Superset connects to Gold-layer Delta tables via a **DuckDB SQLAlchemy driver**. A `superset_config.py` pool event hook runs `INSTALL httpfs; LOAD httpfs; INSTALL delta; LOAD delta;` and configures the MinIO S3 secret on every new connection — this means virtual datasets use plain `SELECT * FROM delta_scan('s3://gold/...')` SQL without manual extension setup.

The `setup_superset_dashboard.py` script provisions datasets, charts, and the *Folkeregister Oversikt* dashboard programmatically via the Superset REST API.

---

## 14. Authentication and Access Control

The portal implements a **JWT + cookie-based** authentication system backed by SQLite.

### Roles

| Role | Capabilities |
|------|-------------|
| `admin` | Full access to all products, users, and admin UI |
| `user` | Access to public products; can request access to restricted products |
| Domain privilege | Granted per domain by admin; enables restricted product access within domain |

### Access Control on Data Products

- `public` products: visible to all authenticated and unauthenticated users
- `restricted` products: require an approved access request or domain privilege
- API key (`X-API-Key: <PORTAL_API_KEY>`): grants full access (for machine-to-machine use, e.g., Jupyter → Portal)

### Token Lifecycle

Access tokens (JWT, 30-minute expiry) and refresh tokens (opaque, 7-day expiry, stored in SQLite) are issued on login and rotated transparently by the client. Airflow DAG tasks use the shared API key rather than user tokens.

---

## 15. Component Dependency Graph

```
                        ┌─────────────────────┐
                        │       KAFKA          │
                        │  (customermaster ns) │
                        └──────────┬──────────┘
                                   │ CloudEvents
                  ┌────────────────▼────────────────┐
                  │     Spark Streaming IDPs          │
                  │  kafka_to_bronze.py               │
                  │  folkeregister_person_idp.py      │
                  │  (SparkApplication, k8s)          │
                  └────────────────┬────────────────┘
                                   │ Delta append
                  ┌────────────────▼────────────────┐
                  │           BRONZE                  │
                  │      MinIO s3://bronze/            │
                  └────────────────┬────────────────┘
                                   │ Spark batch (Airflow)
                  ┌────────────────▼────────────────┐
                  │           SILVER                  │
                  │  SCD Type 2 / GE validation       │
                  │      MinIO s3://silver/            │
                  └────────────────┬────────────────┘
                                   │ Spark batch (Airflow)
                  ┌────────────────▼────────────────┐
                  │            GOLD                   │
                  │  Aggregations / Dimensional       │
                  │       MinIO s3://gold/            │
                  └──────┬──────────────┬───────────┘
                         │              │
              ┌──────────▼───┐    ┌────▼──────────┐
              │   Superset    │    │    Jupyter     │
              │  (Dashboards) │    │  (Notebooks)   │
              └──────────────┘    └───────────────┘
                         │              │
                  ┌──────▼──────────────▼──────┐
                  │         DATA PORTAL         │
                  │  Registry / Catalog / API   │
                  │  Quality / SLA / Lineage    │
                  │  Pipeline Builder           │
                  └─────────────┬──────────────┘
                                │
                  ┌─────────────▼──────────────┐
                  │          AIRFLOW            │
                  │  DAG scheduling             │
                  │  SLA monitor (30 min)       │
                  │  Governance retention       │
                  └────────────────────────────┘
```

### Key Dependency Chains

**New Kafka IDP (self-service path):**
```
Portal UI → Generated DAG → kubernetes.client → SparkApplication
         → register() → Delta Registry → Portal catalog
```

**Quality feedback loop:**
```
Silver/Gold DAG → GE validation → quality_results/ → Portal quality page
             → anomaly detection → incident auto-creation → Slack alert
```

**SLA monitoring:**
```
02_sla_monitor (30 min) → delta-rs DeltaTable.history() → sla_results/
                       → Portal SLA card (compliance %, MTTR)
```

**Data consumption:**
```
Consumer → Portal product page → slettix_client.get_manifest()
        → DuckDB delta_scan() → Jupyter notebook
        → Superset virtual dataset → Dashboard
```

---

*This document reflects the platform state as of April 2026. Architecture diagrams are text-based to remain version-control friendly.*
