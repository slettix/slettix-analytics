#!/usr/bin/env bash
# smoke-test-buzz.sh — Ende-til-ende-test for BUZZ-9 (epic #177)
#
# 5 scenarier mot "What's buzzing"-forsiden:
#   S1: GET /                    → 200 med rendret buzz-HTML
#   S2: GET /api/buzz/snapshot   → JSON med BUZZ-1-skjema (eller graceful tom)
#   S3: GET /api/buzz/personal   → 401 uten auth, 200 med innlogget bruker
#   S4: GET /katalog             → 200 med eksisterende katalog (ikke regredert)
#   S5: Airflow-DAG 03_buzz_metrics → manuell trigger fullfører innen 2 min
#
# Bruk:
#   bash scripts/smoke-test-buzz.sh
#
# Exit-kode 0 ved alle pass, 1 ved feil.

set -euo pipefail

NS="${NS:-slettix-analytics}"
API_KEY="${PORTAL_API_KEY:-dev-key-change-me}"
DATAPORTAL_DEPLOY="deployment/dataportal"

echo "─── BUZZ-9 smoke-test ───"
echo "  namespace : $NS"
echo ""

# ── S1-S4 kjøres inne i dataportal-poden ──────────────────────────────────
kubectl exec -i "$DATAPORTAL_DEPLOY" -n "$NS" -- python3 - "$API_KEY" <<'PYEOF'
import json
import sys
import urllib.error
import urllib.request

API_KEY = sys.argv[1]
BASE    = "http://localhost:8090"
PASS = FAIL = 0
FAIL_DETAILS = []


def call_get(path, headers=None, timeout=30):
    req = urllib.request.Request(BASE + path, headers=headers or {})
    try:
        r = urllib.request.urlopen(req, timeout=timeout)
        return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def report(name, ok, detail):
    global PASS, FAIL
    icon = "✓" if ok else "✗"
    print(f"  {icon} {name}: {'PASS' if ok else 'FAIL'} — {detail}")
    if ok: PASS += 1
    else:
        FAIL += 1
        FAIL_DETAILS.append(f"{name}: {detail}")


# ── S1: GET / → buzz-forside ────────────────────────────────────────────
print("→ S1: GET / (buzz-forside)")
status, body = call_get("/")
html = body.decode("utf-8", errors="ignore")
required = ["Plattform-puls", "Helse-overblikk", "Trender denne uka",
            "Nye publiseringer", "Hva andre spør om", "Trenger oppmerksomhet"]
missing = [m for m in required if m not in html]
if status == 200 and not missing:
    report("S1", True, f"200, {len(html)} bytes, alle 6 widget-titler funnet")
else:
    report("S1", False, f"HTTP {status}, mangler: {missing}")


# ── S2: GET /api/buzz/snapshot ───────────────────────────────────────────
print("→ S2: GET /api/buzz/snapshot")
status, body = call_get("/api/buzz/snapshot")
try:
    data = json.loads(body)
except Exception:
    data = {}
if status == 200:
    if data.get("empty"):
        report("S2", True, "200, graceful tom-tilstand")
    else:
        expected_keys = {"generated_at", "platform_totals", "health",
                         "trending_up", "recent_publications",
                         "popular_questions", "incidents"}
        missing = expected_keys - set(data.keys())
        if not missing:
            totals = data.get("platform_totals", {})
            report("S2", True, f"200, {totals.get('products')} produkter, "
                               f"{len(data.get('incidents', []))} incidents")
        else:
            report("S2", False, f"mangler keys: {missing}")
else:
    report("S2", False, f"HTTP {status}")


# ── S3: GET /api/buzz/personal — 401 uten auth, 200 med token ───────────
print("→ S3: GET /api/buzz/personal")
status, body = call_get("/api/buzz/personal")
if status == 401:
    # Uten auth gir 401 — riktig. Nå test med innlogget bruker.
    sys.path.insert(0, "/opt/dataportal")
    import auth
    admin = next((u for u in auth.list_users() if u["role"] == "admin"), None)
    if admin:
        token = auth.create_access_token(admin["id"], admin["username"], "admin")
        status2, body2 = call_get("/api/buzz/personal",
                                   headers={"Cookie": f"access_token={token}"})
        if status2 == 200:
            try:
                d = json.loads(body2)
                required_fields = {"products_viewed_7d", "new_to_user_7d",
                                   "questions_asked_7d", "top_domains", "is_new_user"}
                missing = required_fields - set(d.keys())
                if not missing:
                    report("S3", True, f"401 uten auth, 200 med admin "
                                       f"(is_new_user={d['is_new_user']})")
                else:
                    report("S3", False, f"mangler felter: {missing}")
            except Exception as exc:
                report("S3", False, f"kunne ikke parse JSON: {exc}")
        else:
            report("S3", False, f"med token: HTTP {status2}")
    else:
        report("S3", True, "401 OK, men admin-bruker mangler så ingen pos-test")
else:
    report("S3", False, f"forventet 401 uten auth, fikk {status}")


# ── S4: GET /katalog — eksisterende katalog ikke regredert ──────────────
print("→ S4: GET /katalog")
status, body = call_get("/katalog")
html = body.decode("utf-8", errors="ignore")
if status == 200 and "Datakatalog" in html:
    report("S4", True, f"200, {len(html)} bytes, 'Datakatalog'-tittel til stede")
else:
    report("S4", False, f"HTTP {status}, 'Datakatalog' i body: {'Datakatalog' in html}")


# ── Subtotal ───────────────────────────────────────────────────────────
print()
print(f"S1-S4: {PASS} PASS, {FAIL} FAIL")
if FAIL_DETAILS:
    for d in FAIL_DETAILS:
        print(f"  - {d}")
    sys.exit(1)
sys.exit(0)
PYEOF

S1_S4_EXIT=$?

# ── S5: Airflow-DAG kan trigges manuelt og fullfører innen 2 min ─────────
echo ""
echo "→ S5: Airflow-DAG 03_buzz_metrics"
SCHED=$(kubectl get pod -n "$NS" -l component=scheduler -o jsonpath='{.items[0].metadata.name}')
RUN_ID="smoke__$(date -u +%Y%m%dT%H%M%SZ)"
kubectl exec -n "$NS" "$SCHED" -c scheduler -- \
  airflow dags trigger 03_buzz_metrics --run-id "$RUN_ID" > /dev/null 2>&1

START=$(date +%s)
S5_PASS=0
while true; do
  STATE=$(kubectl exec -n "$NS" "$SCHED" -c scheduler -- \
    airflow dags list-runs -d 03_buzz_metrics 2>/dev/null \
    | grep "$RUN_ID" | awk -F'|' '{print $3}' | tr -d ' ' | head -1)
  if [ "$STATE" = "success" ]; then
    ELAPSED=$(( $(date +%s) - START ))
    echo "  ✓ S5: PASS — DAG fullført på ${ELAPSED}s"
    S5_PASS=1
    break
  fi
  if [ "$STATE" = "failed" ]; then
    echo "  ✗ S5: FAIL — DAG endte i failed-state"
    break
  fi
  ELAPSED=$(( $(date +%s) - START ))
  if [ "$ELAPSED" -gt 120 ]; then
    echo "  ✗ S5: FAIL — DAG ikke fullført innen 2 min (state=$STATE)"
    break
  fi
  sleep 5
done

# ── Sammenfatning ────────────────────────────────────────────────────────
echo ""
TOTAL_PASS=0
TOTAL_FAIL=0
if [ "$S1_S4_EXIT" -eq 0 ]; then TOTAL_PASS=$((TOTAL_PASS + 4)); else TOTAL_FAIL=$((TOTAL_FAIL + 1)); fi
if [ "$S5_PASS" -eq 1 ]; then TOTAL_PASS=$((TOTAL_PASS + 1)); else TOTAL_FAIL=$((TOTAL_FAIL + 1)); fi

echo "─── BUZZ-9 sammenfatning ───"
if [ "$TOTAL_FAIL" -eq 0 ]; then
  echo "─── SMOKE-TEST: PASS (5/5) ───"
  exit 0
else
  echo "─── SMOKE-TEST: FAIL ($TOTAL_PASS PASS, $TOTAL_FAIL FAIL) ───"
  exit 1
fi
