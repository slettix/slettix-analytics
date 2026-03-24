# Slettix Analytics

Lokal analyseplattform for å utforske Spark, Delta Lake og moderne datateknologier.

## Arkitektur

```
MinIO (S3)
 ├── raw/      – kildedata
 ├── bronze/   – rå Delta-tabeller
 ├── silver/   – renset og validert data
 └── gold/     – aggregerte, business-ready datasett

Apache Spark (master + worker)
Apache Airflow (orkestrering) – kommer i Epic 5
DuckDB / Trino (spørrelag)   – kommer i Epic 6
```

## Kom i gang

### Forutsetninger
- Docker + Docker Compose

### Start stacken

```bash
cp .env.example .env       # Juster passord ved behov
docker compose up --build
```

| Tjeneste       | URL                     |
|----------------|-------------------------|
| Spark Web UI   | http://localhost:8080   |
| MinIO Console  | http://localhost:9001   |
| Jupyter Lab    | http://localhost:8888   |

### Kjør smoke test

Verifiser at Spark kan skrive og lese Delta-tabeller på MinIO:

```bash
docker exec spark-master \
  /opt/spark/bin/spark-submit /opt/spark/jobs/smoke_test.py
```

Forventet output: `>> Smoke test PASSED`

## Prosjektstruktur

```
slettix-analytics/
├── docker-compose.yml
├── docker/spark/Dockerfile     # Spark + Delta Lake + S3A JARs
├── docker/jupyter/Dockerfile   # Jupyter Lab + PySpark-kernel
├── spark/conf/                 # spark-defaults.conf
├── jobs/                       # PySpark-jobber (spark-submit)
└── notebooks/                  # Jupyter-notebooks
    └── 01_getting_started.ipynb
```
