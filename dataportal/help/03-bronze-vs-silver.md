---
title: Hva er forskjellen på Bronze, Silver og Gold?
category: faq
slug: bronze-silver-gold
---

Plattformen bruker **Medallion-arkitekturen** — tre lag der data foredles:

- **Bronze** — råinntak fra Kafka, lagret som hendelser uten transformasjon.
  Brukes til reprosessering.
- **Silver** — rensede og konformerte tabeller. Hovedstedet for
  domeneprodukter som `person_registry`.
- **Gold** — aggregerte, analytiske produkter optimalisert for BI og notebooks.

Som analytiker skal du nesten alltid lese fra **Silver** eller **Gold** —
Bronze er typisk reservert for plattformteam ved feilsøking.

Se [Glossar](/glossary#medallion) for mer.
