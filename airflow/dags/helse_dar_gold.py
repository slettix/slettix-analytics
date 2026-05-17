"""
helse_dar_gold — Airflow DAG for Gold-laget til Dødsårsaksregisteret.

Bygger to Gold-tabeller med ICD-10-berikelse fra silver/helse/icd10:

  • gold/helse/dar/dodsfall_enriched
      Denormalisert fakta — én rad per dødsfall med pre-joinet ICD-10-navn
      og kapittel for underliggende og ekstern årsak (skade + skademekanisme).

  • gold/helse/dar/medvirkende_arsaker
      M:N-bridge — én rad per (dødsfall, medvirkende kode) etter at
      JSON-arrayen `medvirkendeArsaker` fra Silver er eksplodert.

Avhengigheter: silver/helse/dar  (helse_dar-DAG-en) og silver/helse/icd10
(helse_silver-DAG-en) må være ferdig før denne kjøres.

Tasks kjøres sekvensielt for å unngå NodeNotReady-press på Docker Desktop-
klusteret (lærdom fra folkeregister_gold). Det er to korte tasks her, så
total kjøretid endres lite av sekvensiering.
"""

import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

SPARK_NS = "slettix-analytics"


def _spark_app(name_prefix: str, job_file: str) -> str:
    return f"""
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: {name_prefix}-{{{{ ds_nodash }}}}
  namespace: {SPARK_NS}
spec:
  type: Python
  mode: cluster
  image: slettix-analytics/spark:3.5.8
  imagePullPolicy: IfNotPresent
  mainApplicationFile: "local:///opt/spark/jobs/{job_file}"
  sparkVersion: "3.5.8"
  restartPolicy:
    type: Never
  sparkConf:
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    "spark.hadoop.fs.s3a.endpoint": "http://minio.slettix-analytics.svc.cluster.local:9000"
    "spark.hadoop.fs.s3a.path.style.access": "true"
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    "spark.sql.shuffle.partitions": "8"
  driver:
    cores: 1
    memory: "1g"
    memoryOverhead: "256m"
    serviceAccount: spark
    labels:
      version: "3.5.8"
    env:
      - name: AWS_ACCESS_KEY_ID
        valueFrom:
          secretKeyRef:
            name: slettix-credentials
            key: minio-root-user
      - name: AWS_SECRET_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: slettix-credentials
            key: minio-root-password
    volumeMounts:
      - name: jobs
        mountPath: /opt/spark/jobs
        readOnly: true
  executor:
    instances: 1
    cores: 1
    memory: "1g"
    memoryOverhead: "256m"
    labels:
      version: "3.5.8"
    env:
      - name: AWS_ACCESS_KEY_ID
        valueFrom:
          secretKeyRef:
            name: slettix-credentials
            key: minio-root-user
      - name: AWS_SECRET_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: slettix-credentials
            key: minio-root-password
    volumeMounts:
      - name: jobs
        mountPath: /opt/spark/jobs
        readOnly: true
  volumes:
    - name: jobs
      configMap:
        name: airflow-jobs
"""


_DODSFALL_ENRICHED_APP    = _spark_app("helse-dar-dodsfall-enriched", "helse_dar_dodsfall_enriched.py")
_MEDVIRKENDE_ARSAKER_APP  = _spark_app("helse-dar-medvirkende",       "helse_dar_medvirkende_arsaker.py")
_VALIDATE_QUALITY_APP     = _spark_app("helse-dar-validate-quality",  "helse_dar_validate_quality.py")


default_args = {
    "owner":               "helse-team",
    "retries":             1,
    "retry_delay":         timedelta(minutes=3),
    "email_on_failure":    False,
}


with DAG(
    dag_id="helse_dar_gold",
    description="DAR Gold: denormalisert dødsfall_enriched + medvirkende-bridge, beriket med ICD-10",
    schedule="@daily",
    start_date=datetime(2024, 1, 15),
    catchup=False,
    default_args=default_args,
    tags=["helse", "dar", "gold", "icd10"],
    doc_md=__doc__,
) as dag:

    dodsfall_enriched = SparkKubernetesOperator(
        task_id="build_dodsfall_enriched",
        namespace=SPARK_NS,
        application_file=_DODSFALL_ENRICHED_APP,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=20),
    )

    medvirkende_arsaker = SparkKubernetesOperator(
        task_id="build_medvirkende_arsaker",
        namespace=SPARK_NS,
        application_file=_MEDVIRKENDE_ARSAKER_APP,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=20),
    )

    validate_quality = SparkKubernetesOperator(
        task_id="validate_quality",
        namespace=SPARK_NS,
        application_file=_VALIDATE_QUALITY_APP,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=15),
    )

    # Sekvensielt — unngår NodeNotReady-press på Docker Desktop-klusteret
    dodsfall_enriched >> medvirkende_arsaker >> validate_quality
