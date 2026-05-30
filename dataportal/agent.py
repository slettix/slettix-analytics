"""
agent — utility-funksjoner for Agentic AI-veiviseren (epic #165).

Ren pure-funksjon-modul:
  • build_agent_prompt — system-prompt for Claude med manifest + sample-rows
  • has_pii / pii_columns — sjekker for personopplysninger i valgte produkter

Orkestratoren (AGENT-1, kommer i main.py) plugger registry/access-funksjoner
inn og kaller disse helperne. Modulen importerer ikke fra main.py — det holder
den testbar uten å sette opp hele FastAPI-konteksten.
"""

from __future__ import annotations

import json
from typing import Any


# 10k tokens er en grov grense — gir hodemonn for spørsmål + svar + tool-use
# på Sonnet (200k context) men holder kostnaden nede.
MAX_PROMPT_TOKENS = 10_000

# Grov tokenisering: 1 token ≈ 4 tegn (engelsk/norsk-snitt). Konservativt nok
# for å unngå å sprenge context-budsjettet før vi sender til Claude.
_CHARS_PER_TOKEN = 4


def _estimate_tokens(text: str) -> int:
    return (len(text) + _CHARS_PER_TOKEN - 1) // _CHARS_PER_TOKEN


# ─── PII-helpers (AGENT-3) ────────────────────────────────────────────────────


def has_pii(manifests: list[dict]) -> bool:
    """True hvis noen av produktene har minst én kolonne markert med pii=True."""
    for m in manifests:
        for field in m.get("schema") or []:
            if field.get("pii") is True:
                return True
    return False


def pii_columns(manifests: list[dict]) -> dict[str, list[str]]:
    """Returnerer {product_id: [kolonnenavn, ...]} for kolonner markert pii=True.

    Brukes av frontend-modalen (AGENT-8) for å vise hvilke kolonner brukeren
    samtykker til når han klikker accept_pii=true."""
    out: dict[str, list[str]] = {}
    for m in manifests:
        cols = [f["name"] for f in (m.get("schema") or []) if f.get("pii") is True]
        if cols:
            out[m["id"]] = cols
    return out


# ─── Gold-kandidat-forslag (AGENT-4) ──────────────────────────────────────────


def _is_gold_path(source_path: str | None) -> bool:
    if not source_path:
        return False
    return source_path.startswith(("s3://gold/", "s3a://gold/"))


def find_candidate_gold_products(
    selected_manifests: list[dict],
    all_products: list[dict],
    max_results: int = 5,
) -> list[dict]:
    """Returnerer Gold-produkter i samme domene som de valgte produktene.

    Brukes av AGENT-4 til å foreslå et lettere alternativ når brukerens valg
    overskrider row-count-grensen. Filtrerer ut de valgte produktene selv så
    vi ikke foreslår det samme tilbake."""
    domains      = {m.get("domain") for m in selected_manifests if m.get("domain")}
    selected_ids = {m["id"] for m in selected_manifests}
    candidates   = [
        {
            "id":          p["id"],
            "name":        p.get("name", p["id"]),
            "domain":      p.get("domain", ""),
            "description": (p.get("description") or "")[:200],
        }
        for p in all_products
        if p["id"] not in selected_ids
        and p.get("domain") in domains
        and _is_gold_path(p.get("source_path"))
    ]
    return candidates[:max_results]


# ─── Prompt-builder (AGENT-2) ─────────────────────────────────────────────────


_SYSTEM_PREAMBLE = """\
Du er en analyse-assistent for Slettix Analytics-plattformen. Din oppgave er å
besvare brukerens spørsmål ved å generere kjørbar Python-kode i en Jupyter-notebook.

Regler:
  1. Bruk slettix_client.get_product(product_id) for å laste data. Den returnerer
     en pandas DataFrame du kan analysere direkte.
  2. Foretrekk Gold > Silver > Bronze hvis flere kilder er aktuelle.
  3. Hold koden under 200 linjer totalt. Del opp i flere celler med markdown
     mellom for forklaring.
  4. Bruk standard pandas/numpy/matplotlib. Ikke installer nye pakker.
  5. Avslutt alltid med en kort markdown-celle som oppsummerer svaret på
     brukerens spørsmål i klart norsk.
  6. Hvis spørsmålet ikke kan besvares med tilgjengelige data, si det
     eksplisitt i en markdown-celle — ikke gjett.

Eksempel-mønster:
  ```python
  from slettix_client import get_product
  df = get_product("folkeregister.person_registry")
  by_alder = df.groupby(pd.cut(df["alder"], bins=[0,18,30,50,70,120])).size()
  ```
"""


def _format_schema_field(field: dict) -> str:
    name        = field.get("name", "?")
    ftype       = field.get("type", "?")
    description = field.get("description") or ""
    pii         = " [PII]" if field.get("pii") else ""
    sensitivity = field.get("sensitivity")
    sens_tag    = f" [{sensitivity}]" if sensitivity else ""
    desc_part   = f" — {description}" if description else ""
    return f"  - {name} ({ftype}){pii}{sens_tag}{desc_part}"


def _format_sample_rows(sample: dict, max_rows: int = 5) -> str:
    """Bygg en lesbar tabell-blokk fra schema_intro.introspect-utdata."""
    if not sample or "columns" not in sample:
        return "  (ingen sample-data tilgjengelig)"
    col_lines = []
    for c in sample["columns"][:30]:  # cap kolonner — ekstreme breddes manifest svulmer
        vals = c.get("sample_values") or []
        vals_str = ", ".join(json.dumps(v, ensure_ascii=False, default=str) for v in vals[:max_rows])
        pii_tag  = " [PII-maskert]" if c.get("pii_hint") else ""
        col_lines.append(f"  - {c['name']}{pii_tag}: {vals_str}")
    return "\n".join(col_lines) if col_lines else "  (tom tabell)"


def _format_product(manifest: dict, sample: dict | None) -> str:
    parts = [
        f"## Dataprodukt: {manifest['id']}",
        f"**Navn:** {manifest.get('name', manifest['id'])}",
        f"**Domene:** {manifest.get('domain', '?')}",
        f"**Eier:** {manifest.get('owner', '?')}",
    ]
    if manifest.get("description"):
        parts.append(f"**Beskrivelse:** {manifest['description']}")
    parts.append(f"**Kilde-sti:** `{manifest.get('source_path', '?')}`")

    schema = manifest.get("schema") or []
    if schema:
        parts.append("\n**Schema (fra manifest):**")
        parts.extend(_format_schema_field(f) for f in schema)

    if sample:
        if "row_count" in sample:
            parts.append(f"\n**Sample fra Delta-tabellen** ({sample.get('row_count', 0)} rader hentet):")
        else:
            parts.append("\n**Sample fra Delta-tabellen:**")
        parts.append(_format_sample_rows(sample))

    source_products = manifest.get("source_products") or []
    if source_products:
        parts.append(f"\n**Avhenger av:** {', '.join(source_products)}")

    return "\n".join(parts)


def _truncate_samples_if_oversized(
    manifests: list[dict],
    samples_by_id: dict[str, dict],
    question: str,
    max_tokens: int,
) -> dict[str, dict]:
    """Hvis full prompt overskrider max_tokens, dropp sample-rader for produkter
    med flest kolonner først. Beholder skjema-metadata uansett — det er det
    viktigste for at agenten ikke skal hallusinerer kolonnenavn.

    Returnerer en (mulig) modifisert kopi av samples_by_id."""
    samples = {pid: s for pid, s in samples_by_id.items() if s is not None}
    while samples and _estimate_tokens(_assemble(manifests, samples, question)) > max_tokens:
        # Dropp sample for produktet med flest kolonner
        biggest = max(samples, key=lambda pid: len((samples[pid] or {}).get("columns") or []))
        del samples[biggest]
    return samples


def _assemble(manifests: list[dict], samples_by_id: dict[str, dict], question: str) -> str:
    sections = [_SYSTEM_PREAMBLE.strip(), ""]
    sections.append(f"# Brukerens spørsmål\n\n{question}\n")
    sections.append(f"# Tilgjengelige dataprodukter ({len(manifests)})\n")
    for m in manifests:
        sections.append(_format_product(m, samples_by_id.get(m["id"])))
        sections.append("")
    sections.append(
        "# Oppgave\n\n"
        "Skriv en Jupyter-notebook (som en sekvens av markdown- og kode-celler) "
        "som besvarer brukerens spørsmål basert på dataproduktene ovenfor. "
        "Hver celle skal markeres tydelig som enten markdown eller kode."
    )
    return "\n".join(sections)


def build_agent_prompt(
    manifests: list[dict],
    samples_by_id: dict[str, dict],
    question: str,
    max_tokens: int = MAX_PROMPT_TOKENS,
) -> str:
    """Bygg system-prompt for Claude basert på valgte produkter.

    Argumenter:
      manifests:     Liste over manifester for produktene brukeren har valgt.
                     Skal allerede være filtrert til kun produkter brukeren har
                     tilgang til (jf. _filter_accessible i main.py).
      samples_by_id: {product_id: schema_intro.introspect-resultat} — kan være
                     tom eller mangle nøkler for produkter uten lesbar data.
      question:      Brukerens spørsmål i fritekst.
      max_tokens:    Token-budsjett. Default 10k — sample-rader dropps for de
                     bredeste tabellene først hvis vi overskrider.

    Returverdi: én streng som brukes som `system`-felt i Anthropic-kallet.
    """
    if not manifests:
        raise ValueError("build_agent_prompt: ingen manifester gitt")
    if not (question or "").strip():
        raise ValueError("build_agent_prompt: tomt spørsmål")

    trimmed_samples = _truncate_samples_if_oversized(
        manifests, samples_by_id or {}, question, max_tokens
    )
    return _assemble(manifests, trimmed_samples, question)
