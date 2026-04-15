#!/usr/bin/env bash
# k8s-up.sh — Start Slettix Analytics på Kubernetes (Docker Desktop)
#
# Bruk:
#   ./scripts/k8s-up.sh            # installer alt
#   ./scripts/k8s-up.sh minio      # installer kun MinIO
#   ./scripts/k8s-up.sh --dry-run  # vis kommandoer uten å kjøre dem
#
# Forutsetninger:
#   - Docker Desktop med Kubernetes aktivert
#   - kubectl og helm installert
#   - .env finnes (se .env.example)
#   - Kjøres fra repo-roten

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

DRY_RUN=false
TARGET="${1:-all}"
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true && TARGET="all"

run() {
  if $DRY_RUN; then
    echo "[dry-run] $*"
  else
    echo "+ $*"
    "$@"
  fi
}

header() { echo; echo "══════════════════════════════════════════"; echo "  $1"; echo "══════════════════════════════════════════"; }

# ── Forutsetningssjekk ────────────────────────────────────────────────────────

if ! kubectl cluster-info &>/dev/null; then
  echo "Feil: kubectl finner ikke et kjørende cluster."
  echo "Aktiver Kubernetes i Docker Desktop under Settings → Kubernetes."
  exit 1
fi

if ! helm version &>/dev/null; then
  echo "Feil: helm ikke installert. Installer via: brew install helm"
  exit 1
fi

CONTEXT=$(kubectl config current-context)
echo "Cluster: $CONTEXT"
if [[ "$CONTEXT" != "docker-desktop" ]]; then
  echo "Advarsel: forventet context 'docker-desktop', men er '$CONTEXT'."
  read -r -p "Fortsett? [y/N] " yn
  [[ "${yn,,}" == "y" ]] || exit 0
fi

# ── Helm-repoer ───────────────────────────────────────────────────────────────

setup_helm_repos() {
  header "Helm-repoer"
  run helm repo add minio       https://charts.min.io/       2>/dev/null || true
  run helm repo add spark-operator https://kubeflow.github.io/spark-operator 2>/dev/null || true
  run helm repo add apache-airflow https://airflow.apache.org 2>/dev/null || true
  run helm repo add superset    https://apache.github.io/superset-helm-chart 2>/dev/null || true
  run helm repo update
}

# ── K8s-1: Namespace, RBAC, ConfigMaps ───────────────────────────────────────

install_foundation() {
  header "K8s-1: Namespace, RBAC og ConfigMaps"
  run kubectl apply -f k8s/namespace.yaml
  run kubectl apply -f k8s/rbac/
  run kubectl apply -f k8s/configmaps/
  run bash scripts/k8s-secrets-create.sh
}

# ── K8s-2: MinIO ─────────────────────────────────────────────────────────────

install_minio() {
  header "K8s-2: MinIO"
  run helm upgrade --install minio minio/minio \
    --namespace slettix-analytics \
    --values k8s/helm/minio/values.yaml \
    --wait \
    --timeout 3m

  echo "Venter på at MinIO-pod skal bli klar..."
  run kubectl rollout status deployment/minio -n slettix-analytics --timeout=120s

  echo "Oppretter mappe-struktur..."
  # Slett eventuell gammel jobb (idempotent re-kjøring)
  kubectl delete job minio-bucket-init -n slettix-analytics --ignore-not-found=true 2>/dev/null || true
  run kubectl apply -f k8s/helm/minio/bucket-init-job.yaml
  run kubectl wait --for=condition=complete job/minio-bucket-init \
    -n slettix-analytics --timeout=120s

  echo
  echo "MinIO klar:"
  echo "  S3 API    : http://localhost:30900"
  echo "  Console   : http://localhost:30901"
  echo "  K8s intern: http://minio.slettix-analytics.svc.cluster.local:9000"
}

# ── K8s-3: Spark Operator ────────────────────────────────────────────────────

install_spark_operator() {
  header "K8s-3: Spark Kubernetes Operator"

  # Bygg image hvis det ikke finnes lokalt
  if ! docker image inspect slettix-analytics/spark:3.5.8 &>/dev/null; then
    echo "Spark-image ikke funnet — bygger slettix-analytics/spark:3.5.8 ..."
    run bash scripts/build-spark-image.sh
  else
    echo "Spark-image slettix-analytics/spark:3.5.8 finnes allerede."
  fi

  run helm upgrade --install spark-operator spark-operator/spark-operator \
    --namespace slettix-analytics \
    --values k8s/helm/spark-operator/values.yaml \
    --wait \
    --timeout 3m

  echo
  echo "Spark Operator klar. Kjør smoke-test:"
  echo "  kubectl apply -f k8s/spark/test-pi.yaml"
  echo "  kubectl get sparkapplication spark-pi-test -n slettix-analytics -w"
}

# ── K8s-4: Airflow ───────────────────────────────────────────────────────────

install_airflow() {
  header "K8s-4: Airflow med KubernetesExecutor"

  if ! docker image inspect slettix-analytics/airflow:2.9.2 &>/dev/null; then
    echo "Airflow-image ikke funnet — bygger slettix-analytics/airflow:2.9.2 ..."
    run bash scripts/build-airflow-image.sh
  else
    echo "Airflow-image slettix-analytics/airflow:2.9.2 finnes allerede."
  fi

  echo "Oppretter DAG hostPath PVC ..."
  run kubectl apply -f k8s/apps/airflow/dags-pvc.yaml

  run helm upgrade --install airflow apache-airflow/airflow \
    --namespace slettix-analytics \
    --values k8s/helm/airflow/values.yaml \
    --wait \
    --timeout 8m

  echo
  echo "Airflow klar:"
  echo "  UI: http://localhost:30808  (admin / admin)"
}

# ── K8s-5: Jupyter ───────────────────────────────────────────────────────────

install_jupyter() {
  header "K8s-5: Jupyter Notebook"

  if ! docker image inspect slettix-analytics/jupyter:3.5.8 &>/dev/null; then
    echo "Jupyter-image ikke funnet — bygger slettix-analytics/jupyter:3.5.8 ..."
    run bash scripts/build-jupyter-image.sh
  else
    echo "Jupyter-image slettix-analytics/jupyter:3.5.8 finnes allerede."
  fi

  run kubectl apply -f k8s/apps/jupyter/notebooks-pvc.yaml
  run kubectl apply -f k8s/apps/jupyter/deployment.yaml
  run kubectl apply -f k8s/apps/jupyter/service.yaml

  echo "Venter på at Jupyter-pod skal bli klar..."
  run kubectl rollout status deployment/jupyter -n slettix-analytics --timeout=120s

  echo
  echo "Jupyter klar: http://localhost:30888"
}

# ── K8s-6: Superset ───────────────────────────────────────────────────────────

install_superset() {
  header "K8s-6: Apache Superset"

  if ! docker image inspect slettix-analytics/superset:4.1.1 &>/dev/null; then
    echo "Superset-image ikke funnet — bygger slettix-analytics/superset:4.1.1 ..."
    run bash scripts/build-superset-image.sh
  else
    echo "Superset-image slettix-analytics/superset:4.1.1 finnes allerede."
  fi

  run kubectl apply -f k8s/apps/superset/pvc.yaml
  run kubectl apply -f k8s/apps/superset/configmap.yaml

  echo "Kjører Superset init-jobb (db migrate, create-admin, register DuckDB) ..."
  kubectl delete job superset-init -n slettix-analytics --ignore-not-found=true 2>/dev/null || true
  run kubectl apply -f k8s/apps/superset/init-job.yaml
  run kubectl wait --for=condition=complete job/superset-init \
    -n slettix-analytics --timeout=120s

  run kubectl apply -f k8s/apps/superset/deployment.yaml
  run kubectl apply -f k8s/apps/superset/service.yaml

  echo "Venter på at Superset-pod skal bli klar..."
  run kubectl rollout status deployment/superset -n slettix-analytics --timeout=180s

  echo
  echo "Superset klar: http://localhost:30088  (admin / admin)"
}

# ── K8s-7: Dataportal ─────────────────────────────────────────────────────────

install_dataportal() {
  header "K8s-7: Slettix Data Portal"

  if ! docker image inspect slettix-analytics/dataportal:1.0.0 &>/dev/null; then
    echo "Dataportal-image ikke funnet — bygger slettix-analytics/dataportal:1.0.0 ..."
    run bash scripts/build-dataportal-image.sh
  else
    echo "Dataportal-image slettix-analytics/dataportal:1.0.0 finnes allerede."
  fi

  run kubectl apply -f k8s/apps/dataportal/deployment.yaml
  run kubectl apply -f k8s/apps/dataportal/service.yaml

  echo "Venter på at Dataportal-pod skal bli klar..."
  run kubectl rollout status deployment/dataportal -n slettix-analytics --timeout=120s

  echo
  echo "Dataportal klar: http://localhost:30090"
}

# ── K8s-8: Folkeregister IDP (streaming SparkApplications) ───────────────────

deploy_idp() {
  header "K8s-8: Folkeregister IDP — streaming SparkApplications"

  run kubectl apply -f k8s/spark/folkeregister_person_idp.yaml
  run kubectl apply -f k8s/spark/folkeregister_family_idp.yaml

  echo
  echo "IDP-jobber deployet. Sjekk status med:"
  echo "  kubectl get sparkapplication -n slettix-analytics"
  echo "  kubectl logs -l app=folkeregister-person-idp -n slettix-analytics --tail=50"
}

# ── Smoke-test ────────────────────────────────────────────────────────────────

run_smoke_test() {
  header "Smoke-test"

  echo "Kjører Pi-applikasjon som Spark smoke-test..."
  kubectl delete sparkapplication spark-pi-test -n slettix-analytics --ignore-not-found=true 2>/dev/null || true
  run kubectl apply -f k8s/spark/test-pi.yaml
  echo "Venter på at Pi-jobb skal fullføres (maks 2 min) ..."
  run kubectl wait --for=condition=Terminating sparkapplication/spark-pi-test \
    -n slettix-analytics --timeout=120s 2>/dev/null || true

  state=$(kubectl get sparkapplication spark-pi-test -n slettix-analytics \
    -o jsonpath='{.status.applicationState.state}' 2>/dev/null || echo "UKJENT")
  if [[ "$state" == "COMPLETED" ]]; then
    echo "Spark smoke-test: OK (COMPLETED)"
  else
    echo "Spark smoke-test: tilstand=$state (sjekk logs hvis ikke COMPLETED)"
  fi

  echo
  echo "Tjeneste-URLs etter vellykket deploy:"
  echo "  Airflow   : http://localhost:30808  (admin / admin)"
  echo "  MinIO UI  : http://localhost:30901  (fra secret)"
  echo "  Jupyter   : http://localhost:30888"
  echo "  Superset  : http://localhost:30088  (admin / admin)"
  echo "  Dataportal: http://localhost:30090"
  echo
  echo "K8s-intern Kafka (fra CustomerMaster-cluster):"
  echo "  kafka-bootstrap.customermaster.svc.cluster.local:9092"
}

# ── Dispatcher ────────────────────────────────────────────────────────────────

setup_helm_repos

case "$TARGET" in
  foundation)     install_foundation ;;
  minio)          install_foundation; install_minio ;;
  spark-operator) install_foundation; install_spark_operator ;;
  airflow)        install_foundation; install_minio; install_spark_operator; install_airflow ;;
  jupyter)        install_foundation; install_jupyter ;;
  superset)       install_foundation; install_superset ;;
  dataportal)     install_foundation; install_dataportal ;;
  idp)            deploy_idp ;;
  smoke-test)     run_smoke_test ;;
  all)
    install_foundation
    install_minio
    install_spark_operator
    install_airflow
    install_jupyter
    install_superset
    install_dataportal
    deploy_idp
    ;;
  *)
    echo "Ukjent target: $TARGET"
    echo "Gyldige targets: foundation | minio | spark-operator | airflow | jupyter | superset | dataportal | idp | smoke-test | all"
    exit 1
    ;;
esac

echo
echo "Ferdig. Sjekk status med:"
echo "  kubectl get all -n slettix-analytics"
echo
echo "Kjør smoke-test for å verifisere Spark:"
echo "  ./scripts/k8s-up.sh smoke-test"
