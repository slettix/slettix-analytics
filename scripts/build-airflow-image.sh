#!/usr/bin/env bash
# build-airflow-image.sh — Bygg Airflow Docker-image for Kubernetes-bruk
#
# Bruk:
#   ./scripts/build-airflow-image.sh            # bygg slettix-analytics/airflow:2.9.2
#   ./scripts/build-airflow-image.sh --no-cache # tving full rebuild
#
# På Docker Desktop er bygde images automatisk tilgjengelige i K8s-clusteret.
# Ingen push til registry nødvendig for lokal bruk.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

IMAGE_NAME="slettix-analytics/airflow"
IMAGE_TAG="2.9.2"
FULL_IMAGE="$IMAGE_NAME:$IMAGE_TAG"
NO_CACHE=""

for arg in "$@"; do
  [[ "$arg" == "--no-cache" ]] && NO_CACHE="--no-cache"
done

echo "Bygger $FULL_IMAGE ..."
echo "Build context: docker/airflow/"
echo

# shellcheck disable=SC2086
docker build $NO_CACHE \
  --tag "$FULL_IMAGE" \
  docker/airflow/

echo
echo "Image bygget: $FULL_IMAGE"
echo
echo "Verifiser pakker i imaget:"
echo "  docker run --rm $FULL_IMAGE pip show apache-airflow-providers-cncf-kubernetes kafka-python"
