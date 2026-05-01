---
title: Hva er schema-kompatibilitet og hvorfor blokkerer det publisering?
category: faq
slug: schema-compatibility
---

Schema-kompatibilitet er regelen som forhindrer at du publiserer en
breaking endring uten varsel. Når du publiserer et oppdatert manifest,
sjekkes det nye skjemaet mot kontraktens kompatibilitetsmodus:

- **BACKWARD** — kun trygge endringer (legge til nullable kolonner)
- **FORWARD** — kun trygge endringer (fjerne nullable kolonner)
- **FULL** — kun additive endringer
- **NONE** — alt er tillatt (men risikabelt)

Hvis sjekken feiler får du `409 Conflict` med detaljer om hvilke
endringer som bryter kontrakten. Løsninger:

1. Øk **major-versjon** og oppdater kompatibilitetsmodus
2. Endre kontrakten til `NONE` (kun ved godkjenning fra eier)
3. Justér skjemaet ditt slik at det er bakoverkompatibelt

Se [Glossar — Schema-kompatibilitet](/glossary#schema-compatibility).
