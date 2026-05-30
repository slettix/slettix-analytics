"""
agent_orchestrator — Claude-kall og notebook-bygging for agentic AI-veiviseren.

V1-MVP-avgrensning: vi GENERERER en notebook og lagrer den til Jupyter, men
eksekverer den IKKE i dataportal-poden. Brukeren får en "Åpne i Jupyter"-lenke
og klikker "Run All" der. In-process Papermill-eksekvering er en V2-story
(unngår behov for å legge til ipykernel/nbclient/papermill-deps i dataportal-
imaget for denne første iterasjonen).

Ren modul uten FastAPI- eller registry-avhengighet. Orchestrator-endepunktet i
main.py plugger sammen registry, access, schema-introspeksjon og denne modulen.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


log = logging.getLogger(__name__)


# ─── Model-katalog ────────────────────────────────────────────────────────────

# Frontend (AGENT-8) sender en av disse kortnøklene; vi mapper til model-ID.
# Veiledningstekst brukes som tooltip i UI for å hjelpe ikke-tekniske brukere.
MODEL_CHOICES: dict[str, dict[str, str]] = {
    "haiku": {
        "model_id":  "claude-haiku-4-5-20251001",
        "label":     "Haiku 4.5 — rask og rimelig",
        "tooltip":   "Raskeste og billigste valget. Godt for enkle aggregeringer og rett-frem-spørsmål mot ett produkt.",
    },
    "sonnet": {
        "model_id":  "claude-sonnet-4-6",
        "label":     "Sonnet 4.6 — anbefalt balanse",
        "tooltip":   "Anbefalt default. God balanse mellom kvalitet og kostnad — passer for de fleste analyser.",
    },
    "opus": {
        "model_id":  "claude-opus-4-7",
        "label":     "Opus 4.7 — kraftigst, men tregere",
        "tooltip":   "Best for komplekse, fler-stegs analyser med joins og avanserte beregninger. Tregere og dyrere — bruk når du virkelig trenger det.",
    },
}

DEFAULT_MODEL_KEY = "sonnet"


def resolve_model_id(model_key: str | None) -> str:
    key = (model_key or DEFAULT_MODEL_KEY).lower()
    if key not in MODEL_CHOICES:
        raise ValueError(
            f"Ugyldig modellvalg '{model_key}'. Gyldige: {', '.join(MODEL_CHOICES.keys())}"
        )
    return MODEL_CHOICES[key]["model_id"]


# ─── Request-context cache (for AGENT-7 feedback-loop) ─────────────────────────


@dataclass
class RequestContext:
    """Holdes i minne 5 min etter ask-kallet så feedback-endepunktet kan slå opp
    full kontekst og lagre den til MinIO."""
    request_id:        str
    user_id:           str | None
    product_ids:       list[str]
    product_versions:  dict[str, str]
    question:          str
    model_key:         str
    model_id:          str
    system_prompt:     str
    generated_code:    str
    answer_preview:    str
    created_at:        float = field(default_factory=time.time)


class _ContextCache:
    """Trådsikker TTL-cache for RequestContext-objekter."""

    def __init__(self, ttl_seconds: int = 300):
        self._ttl   = ttl_seconds
        self._data: dict[str, RequestContext] = {}
        self._lock  = threading.Lock()

    def put(self, ctx: RequestContext) -> None:
        with self._lock:
            self._gc_locked()
            self._data[ctx.request_id] = ctx

    def get(self, request_id: str) -> RequestContext | None:
        with self._lock:
            self._gc_locked()
            return self._data.get(request_id)

    def _gc_locked(self) -> None:
        cutoff = time.time() - self._ttl
        expired = [rid for rid, ctx in self._data.items() if ctx.created_at < cutoff]
        for rid in expired:
            del self._data[rid]

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)


_CACHE = _ContextCache(ttl_seconds=300)


def cache_put(ctx: RequestContext) -> None:
    _CACHE.put(ctx)


def cache_get(request_id: str) -> RequestContext | None:
    return _CACHE.get(request_id)


def cache_size() -> int:
    return len(_CACHE)


# ─── Claude-kall ──────────────────────────────────────────────────────────────


# Krever at Claude returnerer kun JSON i et fast skjema. Vi bruker ikke
# tool-use her — single-shot er enklere å feilsøke i MVP og holder seg innenfor
# token-budsjettet vi har lagt opp til.
_JSON_INSTRUCTION = """\

# Svaret ditt

Returner **kun gyldig JSON** med følgende form, ingen omkringliggende tekst:

```json
{
  "cells": [
    {"type": "markdown", "content": "# Analyse-overskrift"},
    {"type": "code",     "content": "from slettix_client import get_product\\ndf = get_product('...')"},
    {"type": "markdown", "content": "## Resultat\\n\\nKort forklaring her."}
  ],
  "summary": "1-3 setninger oppsummering av forventet svar."
}
```

- Alle code-celler skal være kjørbare Python.
- Vekslende markdown og code-celler gjør notebook-en lesbar.
- Avslutt med en markdown-celle som oppsummerer hovedfunnet.
"""


@dataclass
class AgentResponse:
    cells:         list[dict]    # [{"type": "markdown"|"code", "content": "..."}, ...]
    summary:       str
    raw_text:      str           # Rå tekstrespons fra Claude (for debug)
    input_tokens:  int = 0
    output_tokens: int = 0


_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.S)


def _extract_json(text: str) -> dict:
    """Plukk ut JSON-blokken fra Claude sin respons.

    Claude returnerer noen ganger JSON inne i et ```json ... ```-fence selv om
    vi ber om rent JSON. Vi prøver fence først, så hele teksten.
    """
    m = _FENCE_RE.search(text)
    if m:
        candidate = m.group(1).strip()
    else:
        candidate = text.strip()
    try:
        return json.loads(candidate)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Klarte ikke å parse JSON fra Claude-respons: {exc}. "
            f"Første 200 tegn: {candidate[:200]!r}"
        ) from exc


def _validate_response_shape(data: dict) -> None:
    if not isinstance(data, dict):
        raise ValueError("Forventet JSON-objekt, fikk: " + type(data).__name__)
    cells = data.get("cells")
    if not isinstance(cells, list) or not cells:
        raise ValueError("Mangler 'cells'-array i Claude-respons")
    for i, cell in enumerate(cells):
        if not isinstance(cell, dict):
            raise ValueError(f"Celle {i} er ikke et objekt")
        ctype = cell.get("type")
        if ctype not in ("markdown", "code"):
            raise ValueError(f"Celle {i}: ugyldig type '{ctype}' (må være markdown/code)")
        if not isinstance(cell.get("content"), str):
            raise ValueError(f"Celle {i}: 'content' må være streng")
    if not isinstance(data.get("summary"), str):
        # Tomt sammendrag er OK — vi krever bare at feltet finnes som streng
        if "summary" in data:
            raise ValueError("'summary' må være streng hvis den finnes")
        data["summary"] = ""


def call_claude(
    api_key: str,
    model_id: str,
    system_prompt: str,
    question: str,
    max_tokens: int = 4096,
    client_factory=None,  # for testing — overrid med en mock
) -> AgentResponse:
    """Kall Claude og returner strukturert AgentResponse.

    Heves ved nettverksfeil, ugyldig JSON eller skjema-brudd.
    """
    if client_factory is None:
        import anthropic
        client_factory = lambda: anthropic.Anthropic(api_key=api_key)

    client = client_factory()
    full_prompt = system_prompt + "\n\n" + _JSON_INSTRUCTION

    message = client.messages.create(
        model=model_id,
        max_tokens=max_tokens,
        system=full_prompt,
        messages=[{"role": "user", "content": question}],
    )

    # Anthropic SDK v0.30+ returnerer content som liste av blokker
    text_parts = [b.text for b in message.content if getattr(b, "type", None) == "text"]
    raw = "\n".join(text_parts).strip()

    data = _extract_json(raw)
    _validate_response_shape(data)

    usage = getattr(message, "usage", None)
    return AgentResponse(
        cells=         data["cells"],
        summary=       data["summary"],
        raw_text=      raw,
        input_tokens=  getattr(usage, "input_tokens", 0) or 0,
        output_tokens= getattr(usage, "output_tokens", 0) or 0,
    )


# ─── Notebook-bygging ─────────────────────────────────────────────────────────


def build_notebook(cells: list[dict], question: str, product_ids: list[str]) -> dict:
    """Konstruer en .ipynb-struktur (nbformat 4) fra agent-genererte celler.

    Topp-celler legges til automatisk: en markdown-overskrift som dokumenterer
    spørsmålet og produktene, og en setup-celle som monterer slettix_client.
    """
    header_md = (
        f"# Agent-generert analyse\n\n"
        f"**Spørsmål:** {question}\n\n"
        f"**Dataprodukter:** {', '.join(product_ids)}\n\n"
        f"_Generert av Slettix Analytics agentic-veiviseren. "
        f"Du kan redigere og kjøre cellene under fritt._"
    )

    setup_code = (
        "# Setup — slettix_client gir tilgang til registrerte dataprodukter\n"
        "import pandas as pd\n"
        "import numpy as np\n"
        "import matplotlib.pyplot as plt\n"
        "from slettix_client import get_product\n"
    )

    nb_cells: list[dict] = [
        _nb_cell("markdown", header_md),
        _nb_cell("code",     setup_code),
    ]
    for c in cells:
        nb_cells.append(_nb_cell(c["type"], c["content"]))

    return {
        "nbformat":       4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language":     "python",
                "name":         "python3",
            },
            "language_info": {"name": "python", "version": "3.11"},
            "slettix_agent": {
                "generated_for_question": question,
                "product_ids":            product_ids,
            },
        },
        "cells": nb_cells,
    }


def _nb_cell(cell_type: str, content: str) -> dict:
    if cell_type == "markdown":
        return {
            "cell_type": "markdown",
            "metadata":  {},
            "source":    content.splitlines(keepends=True) or [""],
        }
    return {
        "cell_type":       "code",
        "metadata":        {},
        "source":          content.splitlines(keepends=True) or [""],
        "execution_count": None,
        "outputs":         [],
    }


def make_request_id() -> str:
    """Kort, URL-trygg unik ID for å koble feedback til en request."""
    return uuid.uuid4().hex[:16]


def extract_generated_code(cells: list[dict]) -> str:
    """Slå sammen alle kode-celler til én streng (for feedback-logging)."""
    return "\n\n".join(c["content"] for c in cells if c.get("type") == "code")


# ─── Row-count-vakt (AGENT-4) ─────────────────────────────────────────────────

# Hvis sum av valgte produkters rader overskrider denne grensen, returner 413
# i stedet for å forsøke å eksekvere — agenten ville sannsynligvis treffe
# minne-grenser eller bruke uforholdsmessig mye tid og tokens.
ROW_COUNT_LIMIT = 1_000_000


class _RowCountCache:
    """Trådsikker TTL-cache for row-counts. 5 min holder for å unngå gjentatt
    MinIO-trafikk når flere agent-spørsmål kommer mot samme produkt på rad."""

    def __init__(self, ttl_seconds: int = 300):
        self._ttl   = ttl_seconds
        self._data: dict[str, tuple[int, float]] = {}
        self._lock  = threading.Lock()

    def get(self, source_path: str) -> int | None:
        with self._lock:
            entry = self._data.get(source_path)
            if entry is None:
                return None
            count, ts = entry
            if time.time() - ts > self._ttl:
                del self._data[source_path]
                return None
            return count

    def put(self, source_path: str, count: int) -> None:
        with self._lock:
            self._data[source_path] = (count, time.time())

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


_ROW_COUNT_CACHE = _RowCountCache(ttl_seconds=300)


def _estimate_row_count_uncached(source_path: str, storage_options: dict) -> int | None:
    """Les rad-antall fra Delta-tabellens parquet-metadata (parquet footer,
    ingen full data-skanning). Returner None ved feil — kallekoden behandler
    None som ukjent og dropper grensen, siden vi heller vil tillate enn å
    blokkere ved transient lesefeil."""
    try:
        from deltalake import DeltaTable
        dt      = DeltaTable(source_path, storage_options=storage_options)
        # to_pyarrow_dataset().count_rows() leser kun parquet footer-metadata,
        # ikke datablokker. Raskt selv for tabeller på millioner av rader.
        return dt.to_pyarrow_dataset().count_rows()
    except Exception as exc:
        log.warning("Kunne ikke estimere rader for %s: %s", source_path, exc)
        return None


def estimate_row_count(
    source_path: str,
    storage_options: dict,
    use_cache: bool = True,
) -> int | None:
    """Hent (cachet) approx row-count for en Delta-tabell."""
    if use_cache:
        cached = _ROW_COUNT_CACHE.get(source_path)
        if cached is not None:
            return cached
    count = _estimate_row_count_uncached(source_path, storage_options)
    if count is not None and use_cache:
        _ROW_COUNT_CACHE.put(source_path, count)
    return count


# ─── Forklar-produkt-prompt (AGENT-5) ─────────────────────────────────────────


_EXPLAIN_SYSTEM_PROMPT = """\
Du forklarer dataprodukter for analytikere på en norsk dataplattform. Brukerne
er domeneeksperter — gi en konsis, lesbar oversikt så de raskt forstår hva de
ser på og om det er nyttig for dem.

Returner **kun markdown** med følgende seksjoner i denne rekkefølgen:

## Hva er dette?
1-2 setninger om hva produktet inneholder og hvor data kommer fra.

## Hvem eier det og hvor ofte oppdateres det?
Eier, domene, oppdateringsfrekvens (basert på SLA hvis tilgjengelig).

## Typiske brukstilfeller
2-3 eksempler på hva man kan analysere med dette produktet, formulert som
korte spørsmål eller scenarier.

## Koblinger til andre produkter
Hvis lineage finnes: nevn kilde-produkter (oppstrøm) og evt. avledede produkter.

Hold svaret under 350 ord. Bruk klart norsk. Ikke gjenta produktets ID i hver
seksjon — anta at det er kjent fra konteksten.
"""


def call_claude_explain(
    api_key: str,
    manifest: dict,
    sample: dict | None,
) -> tuple[str, int, int]:
    """Kall Claude (Haiku) for å generere en produkt-forklaring.

    Returnerer (markdown, input_tokens, output_tokens). Bruker Haiku som default
    siden oppgaven er deskriptiv og krever ikke kompleks resonnering.
    """
    import anthropic

    # Bygg en kompakt produkt-kontekst — manifest + skjema-snutt + ev. sample
    schema_summary = "\n".join(
        f"- {f.get('name', '?')} ({f.get('type', '?')})"
        + (f" — {f.get('description')}" if f.get("description") else "")
        + (" [PII]" if f.get("pii") else "")
        for f in (manifest.get("schema") or [])[:30]
    ) or "(intet skjema definert)"

    sla = manifest.get("quality_sla") or {}
    freshness = sla.get("freshness_hours")

    user_msg_parts = [
        f"**Produkt-ID:** {manifest.get('id', '?')}",
        f"**Navn:** {manifest.get('name', '?')}",
        f"**Domene:** {manifest.get('domain', '?')}",
        f"**Eier:** {manifest.get('owner', '?')}",
        f"**Versjon:** {manifest.get('version', '?')}",
    ]
    if manifest.get("description"):
        user_msg_parts.append(f"**Beskrivelse fra manifest:** {manifest['description']}")
    if freshness:
        user_msg_parts.append(f"**SLA-ferskhet:** {freshness} timer")
    user_msg_parts.append(f"**Kilde-sti:** {manifest.get('source_path', '?')}")
    if manifest.get("source_products"):
        user_msg_parts.append(f"**Avhenger av:** {', '.join(manifest['source_products'])}")
    user_msg_parts.append(f"\n**Skjema:**\n{schema_summary}")

    if sample and sample.get("columns"):
        sample_lines = []
        for c in sample["columns"][:15]:
            vals = c.get("sample_values") or []
            vals_str = ", ".join(repr(v) for v in vals[:3])
            sample_lines.append(f"- {c['name']}: {vals_str}")
        user_msg_parts.append("\n**Eksempel-verdier:**\n" + "\n".join(sample_lines))

    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        system=_EXPLAIN_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": "\n".join(user_msg_parts)}],
    )

    text_parts = [b.text for b in message.content if getattr(b, "type", None) == "text"]
    markdown = "\n".join(text_parts).strip()
    usage = getattr(message, "usage", None)
    return (
        markdown,
        getattr(usage, "input_tokens", 0) or 0,
        getattr(usage, "output_tokens", 0) or 0,
    )
