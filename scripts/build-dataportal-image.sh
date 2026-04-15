#!/usr/bin/env bash
# build-dataportal-image.sh — Bygg Dataportal Docker-image for Kubernetes-bruk
set -euo pipefail
cd "$(dirname "$0")/.."
IMAGE="slettix-analytics/dataportal:1.0.0"
NO_CACHE=""
for arg in "$@"; do [[ "$arg" == "--no-cache" ]] && NO_CACHE="--no-cache"; done
echo "Bygger $IMAGE ..."
# shellcheck disable=SC2086
docker build $NO_CACHE --tag "$IMAGE" --file docker/dataportal/Dockerfile .
echo "Image bygget: $IMAGE"
echo "Tilgjengelig på: http://localhost:30090  (krever Secret og konfigurasjon)"
