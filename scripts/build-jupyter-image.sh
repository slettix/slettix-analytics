#!/usr/bin/env bash
# build-jupyter-image.sh — Bygg Jupyter Docker-image for Kubernetes-bruk
set -euo pipefail
cd "$(dirname "$0")/.."
IMAGE="slettix-analytics/jupyter:3.5.8"
NO_CACHE=""
for arg in "$@"; do [[ "$arg" == "--no-cache" ]] && NO_CACHE="--no-cache"; done
echo "Bygger $IMAGE ..."
# shellcheck disable=SC2086
docker build $NO_CACHE --tag "$IMAGE" -f docker/jupyter/Dockerfile .
echo "Image bygget: $IMAGE"
echo "Tilgjengelig på: http://localhost:30888  (etter kubectl apply -f k8s/apps/jupyter/)"
