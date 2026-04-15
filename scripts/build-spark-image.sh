#!/usr/bin/env bash
# build-spark-image.sh — Bygg Spark Docker-image for Kubernetes-bruk
#
# Bruk:
#   ./scripts/build-spark-image.sh            # bygg og last inn i K8s
#   ./scripts/build-spark-image.sh --no-cache # tving full nedlasting av JARs
#   ./scripts/build-spark-image.sh --no-push  # bygg uten å laste inn i K8s
#
# Docker Desktop deler IKKE automatisk image-cache mellom macOS Docker-daemonen
# og Kubernetes sin containerd (k8s.io-namespace). Dette scriptet laster
# imaget eksplisitt inn i alle K8s-noder etter bygg.
#
# NB: Image-bygget laster ned ~200 MB JARs fra Maven Central ved første bygg.
# Etterfølgende bygg er raske pga. Docker layer cache.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

IMAGE_NAME="slettix-analytics/spark"
IMAGE_TAG="3.5.8"
FULL_IMAGE="$IMAGE_NAME:$IMAGE_TAG"
NO_CACHE=""
NO_PUSH=false

for arg in "$@"; do
  [[ "$arg" == "--no-cache" ]] && NO_CACHE="--no-cache"
  [[ "$arg" == "--no-push"  ]] && NO_PUSH=true
done

echo "Bygger $FULL_IMAGE ..."
echo "Build context: . (repo-rot, pga. COPY jobs/ og conf/)"
echo

# shellcheck disable=SC2086
docker build $NO_CACHE \
  --tag "$FULL_IMAGE" \
  --file docker/spark/Dockerfile \
  .

echo
echo "Image bygget: $FULL_IMAGE"

if $NO_PUSH; then
  echo "(--no-push: hopper over innlasting i Kubernetes)"
else
  echo
  echo "Laster image inn i Kubernetes-noder (k8s.io-namespace)..."
  K8S_NODES=$(kubectl get nodes --no-headers 2>/dev/null | grep -v "control-plane" | awk '{print $1}')

  if [[ -z "$K8S_NODES" ]]; then
    echo "  Ingen worker-noder funnet — hopper over."
  else
    for NODE in $K8S_NODES; do
      echo -n "  → $NODE ... "
      docker save "$FULL_IMAGE" | docker exec -i "$NODE" ctr -n k8s.io images import - > /dev/null 2>&1 \
        && echo "OK" \
        || echo "FEIL (sjekk at docker exec funger mot noden)"
    done
  fi
fi

echo
echo "Verifiser JARs i imaget:"
echo "  docker run --rm $FULL_IMAGE ls /opt/spark/jars/ | grep -E 'kafka|delta|hadoop'"
echo
echo "Test på Kubernetes:"
echo "  kubectl apply -f k8s/spark/test-pi.yaml"
echo "  kubectl get sparkapplication spark-pi-test -n slettix-analytics -w"
