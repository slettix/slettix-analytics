#!/usr/bin/env bash
# k8s-secrets-create.sh — Generer K8s Secret slettix-credentials fra .env-fil
#
# Bruk:
#   ./scripts/k8s-secrets-create.sh             # leser fra .env
#   ./scripts/k8s-secrets-create.sh --dry-run   # skriv ut Secret-YAML uten å applye
#
# Forutsetninger:
#   - kubectl konfigurert mot riktig cluster (docker-desktop)
#   - Namespace slettix-analytics finnes (kjør k8s-up.sh først)
#   - .env eksisterer i repo-roten (se .env.example)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"
NAMESPACE="slettix-analytics"
SECRET_NAME="slettix-credentials"
DRY_RUN=false

for arg in "$@"; do
  [[ "$arg" == "--dry-run" ]] && DRY_RUN=true
done

# ── Les .env ──────────────────────────────────────────────────────────────────

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Feil: $ENV_FILE ikke funnet."
  echo "Opprett den fra .env.example: cp .env.example .env"
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

# ── Sett defaults ─────────────────────────────────────────────────────────────

MINIO_ROOT_USER="${MINIO_ROOT_USER:-admin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-changeme}"
PORTAL_API_KEY="${PORTAL_API_KEY:-dev-key-change-me}"
ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-}"

# Generer Airflow-nøkler hvis de ikke er satt
if [[ -z "${AIRFLOW_FERNET_KEY:-}" ]]; then
  echo "AIRFLOW_FERNET_KEY ikke satt — genererer ny..."
  # Fernet-nøkkel = 32 tilfeldige bytes, url-safe base64-encodet (ingen cryptography-avhengighet)
  AIRFLOW_FERNET_KEY=$(python3 -c "import os, base64; print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
  echo "  Legg til i .env: AIRFLOW_FERNET_KEY=$AIRFLOW_FERNET_KEY"
fi

if [[ -z "${AIRFLOW_SECRET_KEY:-}" ]]; then
  echo "AIRFLOW_SECRET_KEY ikke satt — genererer ny..."
  AIRFLOW_SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))" 2>/dev/null \
    || python -c "import secrets; print(secrets.token_hex(32))")
  echo "  Legg til i .env: AIRFLOW_SECRET_KEY=$AIRFLOW_SECRET_KEY"
fi

# ── Bygg kubectl-kommando ─────────────────────────────────────────────────────

CMD=(
  kubectl create secret generic "$SECRET_NAME"
  --namespace "$NAMESPACE"
  --from-literal="minio-root-user=$MINIO_ROOT_USER"
  --from-literal="minio-root-password=$MINIO_ROOT_PASSWORD"
  --from-literal="airflow-fernet-key=$AIRFLOW_FERNET_KEY"
  --from-literal="airflow-secret-key=$AIRFLOW_SECRET_KEY"
  --from-literal="portal-api-key=$PORTAL_API_KEY"
  --from-literal="anthropic-api-key=$ANTHROPIC_API_KEY"
  --save-config
  --dry-run=client
  -o yaml
)

if $DRY_RUN; then
  echo "--- Dry run — Secret-YAML (verdier er base64-encodet i et ekte Secret) ---"
  "${CMD[@]}"
  exit 0
fi

# Apply via pipe for å unngå at verdier havner i shell-historikken
echo "Oppretter/oppdaterer K8s Secret '$SECRET_NAME' i namespace '$NAMESPACE'..."
"${CMD[@]}" | kubectl apply -f -

echo "Done. Verifiser med:"
echo "  kubectl get secret $SECRET_NAME -n $NAMESPACE"
