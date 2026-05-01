"""
GUIDE-10: FAQ og feilløsninger lest fra markdown-filer i help/.

Hver fil har YAML-lignende frontmatter med metadata:

    ---
    title: "Hvor finner jeg eieren av et produkt?"
    category: faq
    slug: find-owner
    ---
    Markdown-body her …

Ved kall hentes filer fra `help/`-katalogen, parses og caches i prosessminne
til oppstart neste gang. Kategorier:

  faq           — ofte stilte spørsmål
  troubleshoot  — kjente feil og hvordan løse dem
"""

import pathlib
import re

import markdown as _md


HELP_DIR = pathlib.Path(__file__).parent / "help"

CATEGORIES = [
    ("faq",          "Ofte stilte spørsmål"),
    ("troubleshoot", "Vanlige feil og løsninger"),
]


_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n(.*)$", re.DOTALL)
_KV_RE = re.compile(r"^([a-zA-Z_]+):\s*(.*)$")


def _parse_one(path: pathlib.Path) -> dict | None:
    text = path.read_text(encoding="utf-8")
    m = _FRONTMATTER_RE.match(text)
    if not m:
        return None
    meta_block, body = m.group(1), m.group(2)
    meta: dict = {}
    for line in meta_block.splitlines():
        kv = _KV_RE.match(line.strip())
        if kv:
            value = kv.group(2).strip().strip('"').strip("'")
            meta[kv.group(1)] = value
    if not all(k in meta for k in ("title", "category", "slug")):
        return None
    body_html = _md.markdown(body, extensions=["fenced_code", "tables"])
    return {
        "slug":     meta["slug"],
        "title":    meta["title"],
        "category": meta["category"],
        "tags":     [t.strip() for t in meta.get("tags", "").split(",") if t.strip()],
        "body":     body_html,
        "search":   (meta["title"] + " " + body).lower(),
    }


_CACHE: list[dict] | None = None


def all_entries() -> list[dict]:
    """Les og cache alle help-entries. Sorterer alfabetisk innen kategori."""
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    if not HELP_DIR.exists():
        _CACHE = []
        return _CACHE
    entries = []
    for path in sorted(HELP_DIR.glob("*.md")):
        entry = _parse_one(path)
        if entry:
            entries.append(entry)
    _CACHE = entries
    return _CACHE


def by_category() -> list[tuple[str, str, list[dict]]]:
    grouped: dict[str, list[dict]] = {c: [] for c, _ in CATEGORIES}
    for e in all_entries():
        cat = e.get("category", "")
        if cat in grouped:
            grouped[cat].append(e)
    for cat in grouped:
        grouped[cat].sort(key=lambda e: e["title"].lower())
    return [(slug, label, grouped[slug]) for slug, label in CATEGORIES if grouped[slug]]
