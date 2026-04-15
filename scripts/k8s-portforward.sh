#!/usr/bin/env bash
# k8s-portforward.sh — Port-forward alle Slettix Analytics-tjenester til localhost
#
# Bruk:
#   ./scripts/k8s-portforward.sh          # start alle direkte port-forwards
#   ./scripts/k8s-portforward.sh ingress  # start ingress-tunnel (krever /etc/hosts-oppføringer)
#   ./scripts/k8s-portforward.sh stop     # stopp alle
#   ./scripts/k8s-portforward.sh status   # vis kjørende forwards
#
# Direkte port-forwards:
#   http://localhost:8080  → Airflow
#   http://localhost:9000  → MinIO S3 API
#   http://localhost:9001  → MinIO Console
#   http://localhost:8888  → Jupyter
#   http://localhost:8088  → Superset
#   http://localhost:8090  → Dataportal
#
# Ingress-modus (legg til i /etc/hosts: 127.0.0.1  *.slettix.local):
#   http://dataportal.slettix.local  → Dataportal
#   http://airflow.slettix.local     → Airflow
#   http://jupyter.slettix.local     → Jupyter
#   http://superset.slettix.local    → Superset
#   http://minio.slettix.local       → MinIO Console

set -euo pipefail

NS="slettix-analytics"
PID_DIR="/tmp/slettix-portforward"
mkdir -p "$PID_DIR"

stop_all() {
  echo "Stopper alle port-forwards..."
  for pid_file in "$PID_DIR"/*.pid; do
    [[ -f "$pid_file" ]] || continue
    pid=$(cat "$pid_file")
    name=$(basename "$pid_file" .pid)
    if kill "$pid" 2>/dev/null; then
      echo "  Stoppet $name (pid $pid)"
    fi
    rm -f "$pid_file"
  done
  echo "Ferdig."
}

status_all() {
  echo "Kjørende port-forwards:"
  local found=false
  for pid_file in "$PID_DIR"/*.pid; do
    [[ -f "$pid_file" ]] || continue
    pid=$(cat "$pid_file")
    name=$(basename "$pid_file" .pid)
    if kill -0 "$pid" 2>/dev/null; then
      echo "  $name  (pid $pid)"
      found=true
    else
      echo "  $name  (STOPPET — fjerner)"
      rm -f "$pid_file"
    fi
  done
  $found || echo "  Ingen aktive forwards."
}

forward() {
  local name=$1
  local resource=$2
  local ports=$3
  local namespace=${4:-$NS}

  local pid_file="$PID_DIR/${name}.pid"

  # Stopp gammel instans
  if [[ -f "$pid_file" ]]; then
    old_pid=$(cat "$pid_file")
    kill "$old_pid" 2>/dev/null || true
    rm -f "$pid_file"
  fi

  kubectl port-forward "$resource" $ports \
    --namespace "$namespace" \
    >/dev/null 2>&1 &

  echo $! > "$pid_file"
  echo "  $name: $ports  (pid $!)"
}

start_ingress() {
  echo "Starter ingress-tunnel (port 80 → ingress-nginx-controller)..."
  echo "Krever at /etc/hosts inneholder:"
  echo "  127.0.0.1  dataportal.slettix.local airflow.slettix.local jupyter.slettix.local superset.slettix.local minio.slettix.local"
  echo

  forward "ingress" "svc/ingress-nginx-controller" "80:80 443:443" "ingress-nginx"

  echo
  echo "Tilgjengelig på:"
  echo "  Dataportal : http://dataportal.slettix.local"
  echo "  Airflow    : http://airflow.slettix.local    (admin / admin)"
  echo "  Jupyter    : http://jupyter.slettix.local"
  echo "  Superset   : http://superset.slettix.local   (admin / admin)"
  echo "  MinIO UI   : http://minio.slettix.local"
  echo
  echo "Stopp med: ./scripts/k8s-portforward.sh stop"
}

case "${1:-start}" in
  stop)
    stop_all
    exit 0
    ;;
  status)
    status_all
    exit 0
    ;;
  ingress)
    start_ingress
    exit 0
    ;;
  start|*)
    echo "Starter port-forwards for Slettix Analytics..."

    # Airflow webserver
    forward "airflow"    "svc/airflow-webserver"  "8080:8080"

    # MinIO
    forward "minio-s3"   "svc/minio"              "9000:9000"
    forward "minio-ui"   "svc/minio-console"      "9001:9001"

    # Jupyter (hvis deployet)
    kubectl get svc jupyter -n "$NS" &>/dev/null && \
      forward "jupyter"  "svc/jupyter"            "8888:8888" || true

    # Superset (hvis deployet)
    kubectl get svc superset -n "$NS" &>/dev/null && \
      forward "superset" "svc/superset"           "8088:8088" || true

    # Dataportal (hvis deployet)
    kubectl get svc dataportal -n "$NS" &>/dev/null && \
      forward "dataportal" "svc/dataportal"       "8090:8090" || true

    echo
    echo "Tilgjengelig på:"
    echo "  Airflow   : http://localhost:8080  (admin / admin)"
    echo "  MinIO UI  : http://localhost:9001"
    echo "  Jupyter   : http://localhost:8888"
    echo "  Superset  : http://localhost:8088  (admin / admin)"
    echo "  Dataportal: http://localhost:8090"
    echo
    echo "Stopp med: ./scripts/k8s-portforward.sh stop"
    ;;
esac
