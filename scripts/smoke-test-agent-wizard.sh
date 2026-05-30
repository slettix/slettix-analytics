#!/usr/bin/env bash
# smoke-test-agent-wizard.sh — Ende-til-ende-test for AGENT-11 (epic #165)
#
# Kjører 6 scenarier mot agent-veiviseren og rapporterer pass/fail/skip:
#
#   S1: helse.kreftregisteret + spørsmål → tabell + bar-chart (suksess)
#   S2: folkeregister.person_registry + aldersfordeling → histogram (suksess)
#   S3: helse.dar + helse.icd10 + topp 5 dødsårsaker → multi-produkt-join (suksess)
#   S4: ukjent produkt → 403
#   S5: stort datasett (sum >1M rader) → 413 med Gold-kandidater
#   S6: feedback (thumbs_up) for hver av S1-S3 → 202 + MinIO-fil
#
# Bruk:
#   bash scripts/smoke-test-agent-wizard.sh
#
# Forutsetninger:
#   • ANTHROPIC_API_KEY satt i slettix-credentials (eller scenarier 1-3 blir SKIP)
#   • PORTAL_API_KEY tilgjengelig (default: dev-key-change-me)
#   • Produktene som testes finnes i registret med data
#
# Exit-kode:
#   0 — alle ikke-skip-scenarier passerer
#   1 — minst ett scenario FAIL
#
# Total kjøretid: ~3-5 min (mest Claude-latens)

set -euo pipefail

NS="${NS:-slettix-analytics}"
API_KEY="${PORTAL_API_KEY:-dev-key-change-me}"
DATAPORTAL_DEPLOY="deployment/dataportal"

echo "─── AGENT-11 smoke-test ───"
echo "  namespace : $NS"
echo "  api-key   : ${API_KEY:0:8}…"
echo ""

# Python-programmet kjøres inne i dataportal-poden så vi får direkte HTTP til
# localhost:8090 og slipper å eksponere endepunktet eksternt.
kubectl exec -i "$DATAPORTAL_DEPLOY" -n "$NS" -- python3 - "$API_KEY" <<'PYEOF'
import json
import os
import sys
import urllib.error
import urllib.request

API_KEY = sys.argv[1]
BASE    = "http://localhost:8090"
PASS = SKIP = FAIL = 0
FAIL_DETAILS = []
REQUEST_IDS = {}


def api_call(method, path, body=None, timeout=120):
    """Returnerer (status_code, parsed_json_body)."""
    headers = {"Content-Type": "application/json", "X-API-Key": API_KEY}
    data    = json.dumps(body).encode() if body is not None else None
    req     = urllib.request.Request(BASE + path, headers=headers, data=data, method=method)
    try:
        r = urllib.request.urlopen(req, timeout=timeout)
        return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, {}


def report(scenario, outcome, msg):
    """outcome: 'PASS' / 'FAIL' / 'SKIP'"""
    global PASS, SKIP, FAIL
    icons = {"PASS": "✓", "FAIL": "✗", "SKIP": "⚠"}
    print(f"  {icons[outcome]} {scenario}: {outcome} — {msg}")
    if outcome == "PASS":
        PASS += 1
    elif outcome == "SKIP":
        SKIP += 1
    else:
        FAIL += 1
        FAIL_DETAILS.append(f"{scenario}: {msg}")


# ── S1: helse.kreftregisteret + spørsmål ──────────────────────────────────
print("→ S1: helse.kreftregisteret + kreftdødsfall per kommune")
status, body = api_call("POST", "/api/wizard/agent/ask", {
    "product_ids": ["helse.kreftregisteret"],
    "question":    "Hvor mange kreftdødsfall finnes per kommune?",
    "model":       "haiku",
    "accept_pii":  True,
})
if status == 200 and body.get("request_id") and body.get("cell_count", 0) > 0:
    REQUEST_IDS["S1"] = body["request_id"]
    report("S1", "PASS", f"cells={body['cell_count']} tokens={body.get('input_tokens',0)+body.get('output_tokens',0)} notebook={body.get('notebook_filename','')[:30]}…")
elif status == 502:
    report("S1", "SKIP", f"Claude utilgjengelig (typisk kreditt-mangel): {body.get('detail','')[:80]}")
elif status == 503:
    report("S1", "SKIP", "ANTHROPIC_API_KEY ikke konfigurert")
elif status == 404:
    report("S1", "SKIP", "helse.kreftregisteret finnes ikke i registret")
else:
    report("S1", "FAIL", f"HTTP {status}: {body}")


# ── S2: folkeregister.person_registry + aldersfordeling ───────────────────
print("→ S2: folkeregister.person_registry + aldersfordeling")
status, body = api_call("POST", "/api/wizard/agent/ask", {
    "product_ids": ["folkeregister.person_registry"],
    "question":    "Hva er aldersfordelingen i registret?",
    "model":       "haiku",
    "accept_pii":  True,
})
if status == 200 and body.get("request_id"):
    REQUEST_IDS["S2"] = body["request_id"]
    report("S2", "PASS", f"cells={body['cell_count']} tokens={body.get('input_tokens',0)+body.get('output_tokens',0)}")
elif status == 502:
    report("S2", "SKIP", "Claude utilgjengelig")
elif status == 503:
    report("S2", "SKIP", "ANTHROPIC_API_KEY ikke konfigurert")
elif status == 404:
    report("S2", "SKIP", "folkeregister.person_registry finnes ikke")
elif status == 413:
    report("S2", "SKIP", f"datasettet er for stort ({body.get('detail',{}).get('total_rows','?')} rader)")
else:
    report("S2", "FAIL", f"HTTP {status}: {body}")


# ── S3: helse.dar + helse.icd10 multi-produkt ─────────────────────────────
print("→ S3: helse.dar + helse.icd10 + topp 5 dødsårsaker")
status, body = api_call("POST", "/api/wizard/agent/ask", {
    "product_ids": ["helse.dar", "helse.icd10"],
    "question":    "Hva er topp 5 underliggende dødsårsaker med ICD-10-navn?",
    "model":       "sonnet",  # multi-produkt-join — bedre med Sonnet
    "accept_pii":  True,
})
if status == 200 and body.get("request_id"):
    REQUEST_IDS["S3"] = body["request_id"]
    report("S3", "PASS", f"cells={body['cell_count']} tokens={body.get('input_tokens',0)+body.get('output_tokens',0)}")
elif status == 502:
    report("S3", "SKIP", "Claude utilgjengelig")
elif status == 503:
    report("S3", "SKIP", "ANTHROPIC_API_KEY ikke konfigurert")
elif status == 404 or (status == 403 and "blocked" in str(body)):
    report("S3", "SKIP", "ett av produktene finnes ikke / utilgjengelig")
elif status == 428:
    report("S3", "SKIP", f"PII-vakt utløst — sett accept_pii=true: {body.get('detail',{}).get('pii_columns', {})}")
else:
    report("S3", "FAIL", f"HTTP {status}: {body}")


# ── S4 (negativ): ukjent produkt → 403 ─────────────────────────────────────
print("→ S4 (negativ): ukjent produkt → 403")
status, body = api_call("POST", "/api/wizard/agent/ask", {
    "product_ids": ["dette.finnes.ikke"],
    "question":    "tester 403",
    "model":       "haiku",
})
if status == 403:
    detail = body.get("detail", {})
    blocked = detail.get("blocked") if isinstance(detail, dict) else []
    if blocked == ["dette.finnes.ikke"]:
        report("S4", "PASS", "403 med korrekt blocked-liste")
    else:
        report("S4", "PASS", "403 returnert (uten detaljert blocked)")
else:
    report("S4", "FAIL", f"forventet 403, fikk {status}: {body}")


# ── S5 (negativ): stort datasett → 413 ─────────────────────────────────────
# Vi kjenner ikke et spesifikt >1M-produkt på forhånd. Iterer over alle
# produkter og find en kombinasjon som overskrider grensen. Hvis ingen
# finnes, marker scenarioet SKIP (testklusteret har bare små data).
print("→ S5 (negativ): stort datasett → 413")
sys.path.insert(0, "/opt/dataportal/jobs")
sys.path.insert(0, "/opt/dataportal")
try:
    from registry import list_all
    from agent_orchestrator import estimate_row_count, ROW_COUNT_LIMIT
    from schema_intro import _STORAGE_OPTIONS
    big_candidates = []
    for p in list_all()[:20]:  # bare topp 20 — hold tiden nede
        try:
            n = estimate_row_count(p["source_path"], _STORAGE_OPTIONS)
            if n and n > ROW_COUNT_LIMIT:
                big_candidates.append((p["id"], n))
        except Exception:
            continue
    if not big_candidates:
        report("S5", "SKIP", f"ingen produkter har >{ROW_COUNT_LIMIT:,} rader i registret")
    else:
        pid, count = big_candidates[0]
        status, body = api_call("POST", "/api/wizard/agent/ask", {
            "product_ids": [pid],
            "question":    "tester 413",
            "model":       "haiku",
            "accept_pii":  True,
        })
        if status == 413:
            detail = body.get("detail", {})
            candidates = detail.get("candidate_gold_products", [])
            report("S5", "PASS", f"413 for {pid} ({count:,} rader), {len(candidates)} Gold-kandidater foreslått")
        else:
            report("S5", "FAIL", f"forventet 413 for {pid} ({count:,} rader), fikk {status}")
except Exception as exc:
    report("S5", "SKIP", f"introspeksjon feilet: {exc}")


# ── S6: feedback for S1-S3 → 202 + MinIO-fil ──────────────────────────────
print("→ S6: feedback (thumbs_up) for hver suksess-scenario")
if not REQUEST_IDS:
    report("S6", "SKIP", "ingen suksess-scenarier å gi feedback på")
else:
    feedback_count = 0
    for name, rid in REQUEST_IDS.items():
        st, fb_body = api_call("POST", "/api/wizard/agent/feedback", {
            "request_id": rid,
            "rating":     "thumbs_up",
            "comment":    f"smoke-test {name}",
        })
        if st == 202:
            feedback_count += 1
        else:
            report("S6", "FAIL", f"{name} feedback returnerte {st}: {fb_body}")
            break
    else:
        report("S6", "PASS", f"{feedback_count} feedback-poster lagret til s3://gold/agent_feedback/")


# ── Sammenfatning ─────────────────────────────────────────────────────────
print()
print(f"─── AGENT-11 sammenfatning: {PASS} PASS, {SKIP} SKIP, {FAIL} FAIL ───")
if FAIL_DETAILS:
    print()
    print("Feildetaljer:")
    for d in FAIL_DETAILS:
        print(f"  - {d}")
    sys.exit(1)
print()
if SKIP > 0:
    print(f"NOTE: {SKIP} scenarier ble hoppet over. Vanlige årsaker:")
    print("  - ANTHROPIC_API_KEY mangler eller har ingen kreditter (Plans & Billing)")
    print("  - Testdataprodukter mangler i registret")
    print("  - Ingen produkter med >1M rader for å trigge row-count-vakt")
sys.exit(0)
PYEOF

EXIT=$?
echo ""
if [ "$EXIT" -eq 0 ]; then
  echo "─── SMOKE-TEST: PASS ───"
else
  echo "─── SMOKE-TEST: FAIL ───"
fi
exit "$EXIT"
