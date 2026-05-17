#!/usr/bin/env bash
# smoke-test-silver-wizard.sh — Ende-til-ende-test for SILVER-6
#
# Kaller /api/wizard/silver/deploy mot et eksisterende Bronze-produkt,
# verifiserer at alle stegene returnerer ok, trigger DAG-en og bekrefter
# at Silver-tabellen blir skrevet. Rydder opp etter testen.
#
# Bruk:
#   bash scripts/smoke-test-silver-wizard.sh
#
# Krever:
#   • Innlogging via PORTAL_API_KEY (eller sett miljøvariabel før kjøring)
#   • Bronze-produktet "folkeregister.person_events" må finnes med data
#
# Exit-kode 0 ved suksess, 1 ved feil.

set -euo pipefail

NS="${NS:-slettix-analytics}"
PORTAL_INTERNAL="http://dataportal.${NS}.svc.cluster.local:8090"
API_KEY="${PORTAL_API_KEY:-dev-key-change-me}"

# Test-fikstur — tilfeldig slug så testene ikke kolliderer
SUFFIX=$(date -u +%s | tail -c 6)
SLUG="smoke-test-${SUFFIX}"
PRODUCT_ID="test.silver_smoke_${SUFFIX}"
DAG_ID="test_silver_smoke_${SUFFIX}"
TARGET_PATH="test/silver_smoke_${SUFFIX}"
SOURCE_PRODUCT="folkeregister.person_events"

echo "─── SILVER-6 smoke-test ───"
echo "  product_id : $PRODUCT_ID"
echo "  dag_id     : $DAG_ID"
echo "  target     : s3a://silver/$TARGET_PATH"
echo ""

# ── Steg 1: kall /api/wizard/silver/deploy ────────────────────────────────
echo "→ Kaller /api/wizard/silver/deploy ..."
PAYLOAD=$(cat <<EOF
{
  "config": {
    "source":      "s3a://bronze/folkeregister/person_events",
    "target":      "s3a://silver/${TARGET_PATH}",
    "primary_key": "citizenId",
    "payload_extract": {
      "from_column": "payload_json",
      "fields": {
        "citizenId":  "\$.citizenId",
        "firstName":  "\$.firstName",
        "lastName":   "\$.lastName"
      }
    },
    "null_handling": { "drop_if_null": ["citizenId"] }
  },
  "manifest": {
    "id":          "${PRODUCT_ID}",
    "name":        "Smoke-test Silver-produkt",
    "domain":      "test",
    "owner":       "platform-team",
    "description": "Auto-generert av SILVER-6 smoke-test"
  },
  "source_product_id": "${SOURCE_PRODUCT}",
  "schedule":          "@daily",
  "overwrite":         true
}
EOF
)

RESPONSE=$(kubectl exec deployment/dataportal -n "$NS" -- python3 -c "
import os, json, httpx
r = httpx.post(
    '${PORTAL_INTERNAL}/api/wizard/silver/deploy',
    json=${PAYLOAD@Q}[0] if False else json.loads('''${PAYLOAD}'''),
    headers={'X-API-Key': '${API_KEY}'},
    timeout=30.0,
)
print(r.status_code)
print(r.text)
" 2>&1)

STATUS=$(echo "$RESPONSE" | head -1)
BODY=$(echo "$RESPONSE" | tail -n +2)

if [ "$STATUS" != "200" ]; then
  echo "  ✗ Deploy feilet — HTTP $STATUS"
  echo "$BODY"
  exit 1
fi

echo "  ✓ HTTP 200"
echo "$BODY" | python3 -c "
import json, sys
data = json.load(sys.stdin)
all_ok = True
for step, info in (data.get('steps') or {}).items():
    ok = info.get('ok') if isinstance(info, dict) else None
    icon = '✓' if ok else '✗'
    print(f'  {icon} step.{step}: {json.dumps(info)[:120]}')
    if ok is False:
        all_ok = False
if not all_ok:
    sys.exit(1)
"

# ── Steg 2: trigger DAG og vent på success ────────────────────────────────
echo ""
echo "→ Trigger DAG-run ..."
RUN_ID="smoke__$(date -u +%Y%m%dT%H%M%SZ)"
kubectl exec deployment/airflow-scheduler -n "$NS" -- airflow dags trigger "$DAG_ID" -r "$RUN_ID" > /dev/null 2>&1

# Vent på terminal state (success/failed/up_for_retry)
echo "  Venter på at bronze_to_silver-task fullfører (timeout 5 min) ..."
DEADLINE=$(($(date +%s) + 300))
TASK_STATE=""
while [ $(date +%s) -lt $DEADLINE ]; do
  TASK_STATE=$(kubectl exec deployment/airflow-scheduler -n "$NS" -- \
    airflow tasks states-for-dag-run "$DAG_ID" "$RUN_ID" 2>&1 \
    | grep bronze_to_silver | awk -F'|' '{print $4}' | tr -d ' ' || echo "")
  if [ -n "$TASK_STATE" ] && echo "$TASK_STATE" | grep -qE "^(success|failed|up_for_retry)$"; then
    break
  fi
  sleep 15
done

if [ "$TASK_STATE" = "success" ]; then
  echo "  ✓ bronze_to_silver task = success"
else
  echo "  ✗ bronze_to_silver task = ${TASK_STATE:-(timeout)}"
  exit 1
fi

# ── Steg 3: verifiser Silver-tabell finnes ────────────────────────────────
echo ""
echo "→ Verifiserer Silver-tabell ..."
SILVER_OK=$(kubectl exec deployment/dataportal -n "$NS" -- python3 -c "
import boto3
from botocore.client import Config
s3 = boto3.client('s3',
    endpoint_url='http://minio.${NS}.svc.cluster.local:9000',
    aws_access_key_id='admin', aws_secret_access_key='changeme',
    config=Config(signature_version='s3v4'))
r = s3.list_objects_v2(Bucket='silver', Prefix='${TARGET_PATH}/')
delta = any('_delta_log' in (o.get('Key') or '') for o in (r.get('Contents') or []))
print('yes' if delta else 'no')
" 2>&1 | tail -1)

if [ "$SILVER_OK" = "yes" ]; then
  echo "  ✓ Delta-tabell skrevet til s3a://silver/$TARGET_PATH"
else
  echo "  ✗ Ingen Delta-tabell funnet"
  exit 1
fi

# ── Steg 4: cleanup ───────────────────────────────────────────────────────
echo ""
echo "→ Cleanup ..."
kubectl exec deployment/airflow-scheduler -n "$NS" -- airflow dags delete "$DAG_ID" --yes > /dev/null 2>&1 || true

kubectl exec deployment/dataportal -n "$NS" -- python3 -c "
import boto3
from botocore.client import Config
s3 = boto3.client('s3',
    endpoint_url='http://minio.${NS}.svc.cluster.local:9000',
    aws_access_key_id='admin', aws_secret_access_key='changeme',
    config=Config(signature_version='s3v4'))

# Slett config
for key in ['silver/${SLUG}/current.json', 'silver/${SLUG}/history.json']:
    try: s3.delete_object(Bucket='config', Key=key)
    except: pass

# Slett Silver-tabell
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket='silver', Prefix='${TARGET_PATH}/'):
    objs = [{'Key': o['Key']} for o in (page.get('Contents') or [])]
    if objs: s3.delete_objects(Bucket='silver', Delete={'Objects': objs})

# Avregistrer
from deltalake import DeltaTable
dt = DeltaTable('s3://gold/data_products', storage_options={
    'AWS_ENDPOINT_URL': 'http://minio.${NS}.svc.cluster.local:9000',
    'AWS_ACCESS_KEY_ID': 'admin', 'AWS_SECRET_ACCESS_KEY': 'changeme',
    'AWS_ALLOW_HTTP': 'true', 'AWS_S3_ALLOW_UNSAFE_RENAME': 'true'})
dt.delete(\"product_id = '${PRODUCT_ID}'\")
" > /dev/null 2>&1 || true

echo "  ✓ Cleanup ferdig"
echo ""
echo "─── SMOKE-TEST: PASS ───"
exit 0
