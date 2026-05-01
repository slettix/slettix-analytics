---
title: Hvor finner jeg credentials for å lese fra Jupyter?
category: faq
slug: jupyter-credentials
---

Notebookene du genererer fra portalen har boilerplate som leser
credentials fra miljøvariablene `AWS_ACCESS_KEY_ID` og
`AWS_SECRET_ACCESS_KEY`. Disse er allerede satt i Jupyter-poden via
ConfigMap, så vanligvis trenger du **ikke å gjøre noe**.

Hvis du kjører lokalt:

```bash
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=changeme
export AWS_ENDPOINT_URL=http://localhost:9000
```

For produksjon: bruk din personlige tilgang via SSO-integrasjonen.
