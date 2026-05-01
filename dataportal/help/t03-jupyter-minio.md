---
title: "Notebook kan ikke koble til MinIO"
category: troubleshoot
slug: jupyter-minio
---

Tre vanlige årsaker:

1. **Feil endpoint-URL**: bruk `http://minio.slettix-analytics.svc.cluster.local:9000`
   internt i Kubernetes, eller `http://localhost:9000` lokalt.
2. **Manglende credentials**: sjekk `AWS_ACCESS_KEY_ID` og `AWS_SECRET_ACCESS_KEY`
   i Jupyter-pod-ens env-vars. De settes via `slettix-credentials`-secret.
3. **MinIO under cold start**: ved første kall etter cluster-oppstart kan
   MinIO trenge ~10-30 sek på å være klar. Forsøk igjen.

Test fra Jupyter:

```python
import boto3
s3 = boto3.client(
    "s3",
    endpoint_url="http://minio.slettix-analytics.svc.cluster.local:9000",
)
print(s3.list_buckets())
```

Forventet output: liste over buckets (`raw`, `bronze`, `silver`, `gold` ...).
