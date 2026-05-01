---
title: "Jeg får 401 Unauthorized fra portal-API"
category: troubleshoot
slug: portal-401
---

Portal-API krever enten:

- **Innlogget bruker** (cookie satt via `/login`)
- **Gyldig X-API-Key** (header `X-API-Key: <key>`)

Sjekk:

```bash
# Verifiser at API-key er korrekt
echo $PORTAL_API_KEY

# Test med curl
curl -H "X-API-Key: $PORTAL_API_KEY" http://localhost:8090/api/products
```

Hvis nøkkelen er korrekt men du fortsatt får 401: pod-en kan ha
restartet og lest en ny `slettix-credentials`-secret. Sjekk
`kubectl describe pod` og restart Airflow scheduler hvis
`PORTAL_API_KEY` er satt fra secret.
