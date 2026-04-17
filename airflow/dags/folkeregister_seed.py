"""
Folkeregister Bronze Seeder DAG — kun for dev/test-miljø

Kjører ein gongs seeder-jobb som fyller Bronze med ~2 000 syntetiske
person- og familiehendelser slik at folkeregister_silver og folkeregister_gold
kan testes utan tilgang til live Kafka-strøm.

Trigges manuelt frå Airflow UI (ingen schedule).

Kjørerekke:
  seed_bronze ──► trigger_silver

Etter fullføring:
  1. folkeregister_silver vil ha Silver-tabellane klare
  2. folkeregister_gold kan køyrast
  3. Notebookane i Jupyter vil få data via get_product()
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

SPARK_NS = "slettix-analytics"

_SEED_APP = f"""
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: fr-seed-bronze-{{{{ ds_nodash }}}}
  namespace: {SPARK_NS}
spec:
  type: Python
  mode: cluster
  image: slettix-analytics/spark:3.5.8
  imagePullPolicy: IfNotPresent
  mainApplicationFile: "local:///opt/spark/jobs/folkeregister_seed_bronze.py"
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
    memory: "512m"
    memoryOverhead: "128m"
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

default_args = {
    "owner":            "slettix",
    "retries":          1,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": False,
    "email_on_retry":   False,
}

with DAG(
    dag_id="folkeregister_seed",
    description="Folkeregister Bronze Seeder — syntetiske testdata (manuell trigger)",
    schedule=None,
    start_date=datetime(2024, 1, 15),
    catchup=False,
    default_args=default_args,
    tags=["folkeregister", "seed", "dev"],
    doc_md=__doc__,
) as dag:

    seed_bronze = SparkKubernetesOperator(
        task_id="seed_bronze",
        namespace=SPARK_NS,
        application_file=_SEED_APP,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=20),
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="folkeregister_silver",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    seed_bronze >> trigger_silver
