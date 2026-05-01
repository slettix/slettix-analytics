---
title: "Spark-jobben min får 403 Forbidden mot MinIO"
category: troubleshoot
slug: spark-403
---

403 Forbidden fra S3A-driver betyr enten manglende eller feil credentials.

Sjekk SparkApplication-spec:

```yaml
driver:
  env:
    - name: AWS_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef: {name: slettix-credentials, key: minio-root-user}
    - name: AWS_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef: {name: slettix-credentials, key: minio-root-password}
executor:
  env:
    # SAMME som driver — executors har egne env-vars
```

**Vanlig glipp**: env satt kun på driver, ikke på executor.

Konfigurer også S3A-credentials provider i `sparkConf`:

```yaml
"spark.hadoop.fs.s3a.aws.credentials.provider":
  "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
```
