#!/usr/bin/env bash
# build-superset-image.sh — Bygg Superset Docker-image for Kubernetes-bruk
set -euo pipefail
cd "$(dirname "$0")/.."
IMAGE="slettix-analytics/superset:4.1.1"
NO_CACHE=""
for arg in "$@"; do [[ "$arg" == "--no-cache" ]] && NO_CACHE="--no-cache"; done
echo "Bygger $IMAGE ..."
# shellcheck disable=SC2086
docker build $NO_CACHE --tag "$IMAGE" docker/superset/
echo "Image bygget: $IMAGE"
echo "Tilgjengelig på: http://localhost:30088  (admin / admin)"
