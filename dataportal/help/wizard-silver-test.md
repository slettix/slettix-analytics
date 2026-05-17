# Ende-til-ende-test: Bronze → Silver-veiviser (SILVER-6)

Repeterbart testscenario som verifiserer at en analytiker kan gå fra et IDP-/Bronze-produkt til en fungerende Silver-pipeline uten kodeendringer.

## Forutsetninger

- Innlogget bruker med skriverettighet (admin eller domeneprivilegier på målets domene)
- IDP-/Bronze-produkt med minst én rad i Bronze-tabellen (f.eks. `folkeregister.person_events` eller `helseregister.dar.events`)
- ICD-10/kommune-referansetabeller på plass (hvis Silver-target trenger join — utenfor scope for V1)

## Manuelt testscenario

### Steg 1 — Start veiviseren

1. Logg inn i portalen
2. Naviger til IDP-produktsiden, f.eks. `/products/helseregister.dar.events`
3. Klikk **«Lag Silver-produkt»** (knapp ved siden av produktnavnet)

### Steg 2 — Schema-introspeksjon

1. Vent på at samplinger lastes (100 rader trekkes via `deltalake-rust`)
2. Verifiser at:
    - Kolonner listes med dattyper og null-rate
    - Hvis Bronze er IDP-format med `payload_json`-kolonne — ekstraherte felter listes separat

### Steg 3 — Velg transformasjonsmønster

- **simple-clean** — flat Bronze, ren cast + null-handling
- **payload-flatten** — IDP-format, `payload_json`-felter ekstraheres til typede kolonner

### Steg 4 — Konfigurer kolonner

For hver kolonne velg:
- Target-navn (default samme som kilde)
- Cast-type: `string` / `integer` / `long` / `double` / `boolean` / `date` / `timestamp`
- `drop_if_null` (boolean — fjern rader der kolonnen er null)
- `fill_default` (verdi til å fylle null)
- `pii` + `sensitivity`-flagg for governance

Velg **primary_key** for Delta-merge.

### Steg 5 — Silver-manifest

Fyll inn:
- `id` (anbefalt: `<domain>.<navn>`, f.eks. `helse.dar`)
- `name`, `description`
- `domain`, `owner`
- `quality_sla.freshness_hours`
- `tags`

### Steg 6 — Forhåndsvis

- JSON-config genereres
- DAG-kode forhåndsvises
- Bekreft eller gå tilbake

### Steg 7 — Deploy

Klikk **Deploy**. Verifiser at responsen viser `ok` på alle steg:

```json
{
  "steps": {
    "save_config":        { "ok": true,  "url": "s3a://config/silver/<slug>/current.json" },
    "generate_dag":       { "ok": true,  "dag_id": "<dag_id>", "path": "/opt/airflow/dags/<dag_id>.py" },
    "configmap_patch":    { "ok": true,  "status": "ok" },
    "scheduler_restart":  { "ok": true,  "status": "ok" },
    "register_manifest":  { "ok": true,  "product_id": "<id>" }
  }
}
```

## Verifikasjon etter deploy

### a) Config i MinIO

```bash
kubectl exec deployment/dataportal -n slettix-analytics -- python3 -c "
import boto3
from botocore.client import Config
s3 = boto3.client('s3',
    endpoint_url='http://minio.slettix-analytics.svc.cluster.local:9000',
    aws_access_key_id='admin', aws_secret_access_key='changeme',
    config=Config(signature_version='s3v4'))
obj = s3.get_object(Bucket='config', Key='silver/<slug>/current.json')
print(obj['Body'].read().decode())
"
```

Forventning: JSON med `slug`, `config` og `saved_at`.

### b) DAG synlig i Airflow

```bash
kubectl exec deployment/airflow-scheduler -n slettix-analytics -- airflow dags list | grep <dag_id>
```

Forventning: `<dag_id>` listes innen ~20 sekunder etter deploy (auto-restart fra #144).

### c) DAG kjører vellykket

```bash
RUN_ID="manual__test_$(date -u +%Y%m%dT%H%M%SZ)"
kubectl exec deployment/airflow-scheduler -n slettix-analytics -- airflow dags trigger <dag_id> -r "$RUN_ID"
# vent ~30-60 sek
kubectl exec deployment/airflow-scheduler -n slettix-analytics -- airflow tasks states-for-dag-run <dag_id> "$RUN_ID"
```

Forventning: `bronze_to_silver`-task → state `success`.

### d) Silver-tabell i MinIO

```bash
kubectl exec deployment/dataportal -n slettix-analytics -- python3 -c "
import boto3
from botocore.client import Config
s3 = boto3.client('s3',
    endpoint_url='http://minio.slettix-analytics.svc.cluster.local:9000',
    aws_access_key_id='admin', aws_secret_access_key='changeme',
    config=Config(signature_version='s3v4'))
r = s3.list_objects_v2(Bucket='silver', Prefix='<target-path>/')
print(f'Objekter: {r.get(\"KeyCount\", 0)}')
for o in (r.get('Contents') or [])[:5]:
    print(f'  {o[\"Key\"]}')
"
```

Forventning: `_delta_log/00000000000000000000.json` finnes + minst én parquet-fil.

### e) Produktet i katalogen

```bash
curl -s http://localhost:30090/api/products/<product_id> | jq '.id, .source_products, .schema | length'
```

Forventning:
- `id` matcher det du valgte i steg 5
- `source_products` inneholder Bronze-produktet
- `schema` har antall kolonner du konfigurerte

## Cleanup

For å rydde opp etter en test (når den brukes som engangs-test):

```bash
SLUG="<slug>"
DAG_ID="<dag_id>"
PRODUCT_ID="<product_id>"
TARGET_PATH="<target-path-uten-bucket>"   # f.eks. "helse/dar"

# 1) Stop og slett alle DAG-runs
kubectl exec deployment/airflow-scheduler -n slettix-analytics -- airflow dags pause "$DAG_ID"
kubectl exec deployment/airflow-scheduler -n slettix-analytics -- airflow dags delete "$DAG_ID" --yes

# 2) Slett DAG fra ConfigMap (krever at man re-genererer airflow-dags uten denne)
#    Enkleste vei: bash scripts/k8s-update-configmaps.sh dags
#    (DAG-filen ligger nå kun i emptyDir på pod-ene, restart fjerner den)

# 3) Slett Silver-config + history i MinIO
kubectl exec deployment/dataportal -n slettix-analytics -- python3 -c "
import boto3
from botocore.client import Config
s3 = boto3.client('s3',
    endpoint_url='http://minio.slettix-analytics.svc.cluster.local:9000',
    aws_access_key_id='admin', aws_secret_access_key='changeme',
    config=Config(signature_version='s3v4'))
for key in [f'silver/$SLUG/current.json', f'silver/$SLUG/history.json']:
    try: s3.delete_object(Bucket='config', Key=key)
    except: pass
"

# 4) Slett Silver-tabellen i MinIO
kubectl exec deployment/dataportal -n slettix-analytics -- python3 -c "
import boto3
from botocore.client import Config
s3 = boto3.client('s3',
    endpoint_url='http://minio.slettix-analytics.svc.cluster.local:9000',
    aws_access_key_id='admin', aws_secret_access_key='changeme',
    config=Config(signature_version='s3v4'))
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket='silver', Prefix=f'$TARGET_PATH/'):
    objs = [{'Key': o['Key']} for o in (page.get('Contents') or [])]
    if objs: s3.delete_objects(Bucket='silver', Delete={'Objects': objs})
"

# 5) Avregistrer produktet fra Delta-registret
kubectl exec deployment/dataportal -n slettix-analytics -- python3 -c "
from deltalake import DeltaTable
dt = DeltaTable('s3://gold/data_products', storage_options={
    'AWS_ENDPOINT_URL': 'http://minio.slettix-analytics.svc.cluster.local:9000',
    'AWS_ACCESS_KEY_ID': 'admin', 'AWS_SECRET_ACCESS_KEY': 'changeme',
    'AWS_ALLOW_HTTP': 'true', 'AWS_S3_ALLOW_UNSAFE_RENAME': 'true'})
dt.delete(\"product_id = '$PRODUCT_ID'\")
"

# 6) Slett quality-/SLA-results om de er generert
kubectl exec deployment/dataportal -n slettix-analytics -- python3 -c "
import boto3
from botocore.client import Config
s3 = boto3.client('s3',
    endpoint_url='http://minio.slettix-analytics.svc.cluster.local:9000',
    aws_access_key_id='admin', aws_secret_access_key='changeme',
    config=Config(signature_version='s3v4'))
for prefix in ['quality_results/$PRODUCT_ID/', 'sla_results/$PRODUCT_ID/']:
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket='gold', Prefix=prefix):
        objs = [{'Key': o['Key']} for o in (page.get('Contents') or [])]
        if objs: s3.delete_objects(Bucket='gold', Delete={'Objects': objs})
"
```

## Automatisert smoke-test

Se `scripts/smoke-test-silver-wizard.sh`. Kjør med:

```bash
bash scripts/smoke-test-silver-wizard.sh
```

Scriptet:
1. Henter en API-token
2. Kaller `/api/wizard/silver/deploy` med en fikset config mot et eksisterende Bronze-produkt
3. Verifiserer at responsen har `ok` på alle steg
4. Sletter ressursene (cleanup-blokken over)

Exit-kode 0 = test lykkes, 1 = noe feilet.
