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

# Last inn i alle k8s-noder.
# Docker Desktop multi-node bruker containerd per node — docker build alene
# er ikke nok. Scriptet laster bildet via en kortlivd privilegert pod.
_load_image_on_nodes() {
  local image="$1"
  local ns="slettix-analytics"
  local tar="/tmp/_dataportal_img.tar"

  echo "Eksporterer $image ..."
  docker save "$image" -o "$tar"

  # Last ned ctr-binary for arm64 om den ikke finnes
  local ctr_bin="/tmp/_ctr_bin"
  if [[ ! -x "$ctr_bin" ]]; then
    echo "Laster ned ctr (arm64) ..."
    curl -sSL \
      "https://github.com/containerd/containerd/releases/download/v2.2.0/containerd-2.2.0-linux-arm64.tar.gz" \
      | tar -C "$(dirname "$ctr_bin")" -xz bin/ctr
    mv /tmp/bin/ctr "$ctr_bin"
    chmod +x "$ctr_bin"
  fi

  # En loader-pod per node som IKKE allerede har nytt image
  for NODE in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    echo "  → Laster inn på node $NODE ..."
    POD="img-loader-$(echo "$NODE" | tr '.' '-')"

    kubectl delete pod "$POD" -n "$ns" --ignore-not-found=true --wait=false 2>/dev/null || true

    # Finn et image som allerede finnes på noden som base for loader-poden
    BASE=$(kubectl get pods -n "$ns" --field-selector spec.nodeName="$NODE" \
           -o jsonpath='{.items[0].spec.containers[0].image}' 2>/dev/null || echo "alpine:3.18")

    kubectl run "$POD" -n "$ns" \
      --image="$BASE" \
      --image-pull-policy=Never \
      --restart=Never \
      --overrides='{
        "spec": {
          "nodeName": "'"$NODE"'",
          "hostPID": true,
          "containers": [{
            "name": "'"$POD"'",
            "image": "'"$BASE"'",
            "imagePullPolicy": "Never",
            "command": ["sleep","300"],
            "securityContext": {"privileged": true},
            "volumeMounts": [{"name":"cs","mountPath":"/run/containerd/containerd.sock"}]
          }],
          "volumes": [{"name":"cs","hostPath":{"path":"/run/containerd/containerd.sock"}}]
        }
      }' 2>/dev/null || true

    # Vent til pod er Running
    kubectl wait pod "$POD" -n "$ns" --for=condition=Ready --timeout=30s 2>/dev/null || true

    kubectl cp "$tar"     "$ns/$POD:/tmp/img.tar"  2>/dev/null || true
    kubectl cp "$ctr_bin" "$ns/$POD:/usr/local/bin/ctr" 2>/dev/null || true
    kubectl exec -n "$ns" "$POD" -- chmod +x /usr/local/bin/ctr 2>/dev/null || true
    kubectl exec -n "$ns" "$POD" -- \
      ctr --address /run/containerd/containerd.sock -n k8s.io images import /tmp/img.tar 2>/dev/null \
      && echo "    ✓ $NODE" || echo "    ✗ $NODE (sjekk manuelt)"

    kubectl delete pod "$POD" -n "$ns" --ignore-not-found=true --wait=false 2>/dev/null || true
  done
  rm -f "$tar"
}

if kubectl cluster-info &>/dev/null; then
  _load_image_on_nodes "$IMAGE"
fi

echo "Tilgjengelig på: http://localhost:30090  (krever Secret og konfigurasjon)"
