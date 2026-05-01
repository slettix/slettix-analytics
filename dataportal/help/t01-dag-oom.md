---
title: "DAG-en min feiler med OOMKilled"
category: troubleshoot
slug: dag-oom
---

OOMKill betyr at en task brukte mer minne enn pod-limit'et. Vanlige årsaker:

- **Pandas på store tabeller** — pandas DataFrame er typisk 5-10x parquet-størrelsen.
  Kjør jobben som Spark istedet (se `jobs/folkeregister_validate_quality.py`
  for eksempel på hvordan validering er flyttet til Spark).
- **`.collect()` i Spark** — drar hele tabellen inn i driver-pod-en.
  Bruk `.write` eller `.toPandas(arrow_enabled=True)` med `.limit()`.
- **For mange parallelle Spark-jobber** — kjør sekvensielt eller gi
  Spark Operator mer minne.

Sjekk pod-status:

```bash
kubectl describe pod -n slettix-analytics <pod-name>
```

Hvis det står `OOMKilled` under `Last State`, øk `memory`-limit i
Helm-values eller spark-app-template.
