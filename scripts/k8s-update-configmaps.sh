#!/usr/bin/env bash
# k8s-update-configmaps.sh — Bygg ConfigMaps på nytt fra kildefiler og restart berørte pods
#
# Bruk:
#   ./scripts/k8s-update-configmaps.sh           # oppdater alle
#   ./scripts/k8s-update-configmaps.sh dags       # kun DAG ConfigMap
#   ./scripts/k8s-update-configmaps.sh jobs       # kun jobs ConfigMap
#   ./scripts/k8s-update-configmaps.sh products   # kun product-manifests
#
# Kjøres fra repo-roten.

set -euo pipefail

NS="slettix-analytics"
TARGET="${1:-all}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

update_dags() {
  echo "→ Oppdaterer airflow-dags ConfigMap (airflow/dags/) …"
  kubectl create configmap airflow-dags \
    --namespace "$NS" \
    --from-file=airflow/dags/ \
    --dry-run=client -o yaml | kubectl apply -f -
  echo "  Restarter Airflow scheduler og webserver …"
  kubectl rollout restart deployment/airflow-scheduler   -n "$NS" 2>/dev/null || true
  kubectl rollout restart deployment/airflow-webserver   -n "$NS" 2>/dev/null || true
  kubectl rollout restart statefulset/airflow-triggerer  -n "$NS" 2>/dev/null || true
  kubectl rollout status  deployment/airflow-scheduler   -n "$NS" --timeout=120s
}

update_jobs() {
  echo "→ Oppdaterer airflow-jobs ConfigMap (jobs/ + conf/products/*.json) …"
  # Kombiner Python-jobber og manifest-JSON i én midlertidig mappe
  # slik at DAG-kode kan lese manifester fra /opt/airflow/jobs/<product_id>.json
  _tmpdir=$(mktemp -d)
  cp jobs/*.py "$_tmpdir/"
  cp conf/products/*.json "$_tmpdir/"
  kubectl create configmap airflow-jobs \
    --namespace "$NS" \
    --from-file="$_tmpdir/" \
    --dry-run=client -o yaml | kubectl apply -f -
  rm -rf "$_tmpdir"
  echo "  (Spark-pods og Airflow-pods laster fra ConfigMap ved kjøretid — ingen restart nødvendig)"
}

update_products() {
  echo "→ Oppdaterer product-manifests ConfigMap (conf/products/) …"
  kubectl create configmap product-manifests \
    --namespace "$NS" \
    --from-file=conf/products/ \
    --dry-run=client -o yaml | kubectl apply -f -

  echo "  Registrerer alle produkt-manifester i Delta Lake-registret …"
  for json_file in conf/products/*.json; do
    product_id=$(python3 -c "import json; print(json.load(open('$json_file'))['id'])")
    kubectl cp "$json_file" \
      "${NS}/$(kubectl get pod -n "$NS" -l app=dataportal -o jsonpath='{.items[0].metadata.name}'):/tmp/$(basename "$json_file")" 2>/dev/null
    kubectl exec deployment/dataportal -n "$NS" -- python3 -c "
import sys, json
sys.path.insert(0, '/opt/dataportal/jobs')
from registry import register
register(json.load(open('/tmp/$(basename "$json_file")')))
" 2>/dev/null
    echo "    registrert: $product_id"
  done
  echo "  Restarter dataportal …"
  kubectl rollout restart deployment/dataportal -n "$NS"
  kubectl rollout status  deployment/dataportal -n "$NS" --timeout=60s
}

case "$TARGET" in
  dags)     update_dags ;;
  jobs)     update_jobs ;;
  products) update_products ;;
  all)
    update_dags
    update_jobs
    update_products
    ;;
  *)
    echo "Ukjent target: $TARGET. Gyldige: dags | jobs | products | all"
    exit 1
    ;;
esac

echo
echo "Ferdig. Verifiser med:"
echo "  kubectl exec deployment/airflow-scheduler -n $NS -- airflow dags list"
echo "  kubectl exec deployment/dataportal -n $NS -- python3 -c 'import sys; sys.path.insert(0,\"/opt/dataportal/jobs\"); from registry import list_all; [print(p[\"id\"]) for p in list_all()]'"
