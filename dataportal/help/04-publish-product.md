---
title: Hvordan publiserer jeg et nytt dataprodukt?
category: faq
slug: publish-product
---

Tre veier, avhengig av produkttypen:

1. **Analytisk produkt** (avledet fra eksisterende data): bruk veiviseren
   under **Lag → Analytisk produkt** i sidemenyen, eller
   `publish_analytical()` fra Jupyter.
2. **Pipeline-basert produkt** (ny Bronze/Silver/Gold-jobb): bruk
   **Bygg → Pipeline-bygger** for å generere DAG og Spark-jobb fra mal.
3. **Manuell registrering**: kall `register(manifest)` direkte fra Jupyter
   eller via `POST /api/products` med X-API-Key.

Alle veier ender med at manifestet registreres i Delta-registret og blir
synlig i katalogen.
