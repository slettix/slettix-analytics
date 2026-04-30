"""
Folkeregister Silver DAG

Transformerer Bronze-tabellene til tre Silver-dataprodukter:

  person_registry    — Autoritativt personregister med SCD Type 2
                       (adresse, sivilstatus, vitalstatus)
  family_relations   — Relasjonsgraf: ekteskap, skilsmisser, foreldre–barn
  residence_history  — Kronologisk bostedshistorikk per person

Kjørerekke:
  build_person_registry ──► build_family_relations ──► build_residence_history ──► register_dag_links

  Kjøres sekvensielt for å unngå at flere spark-submit-prosesser kjører
  samtidig inne i spark-operator-controlleren (medfører OOMKill).

Avhengigheter (kjøres først):
  - folkeregister_idp DAG: Kafka → Bronze streaming-jobber

SparkKubernetesOperator starter SparkApplication-CRD-er i slettix-analytics
namespace. Jobbene er bakt inn i Spark-imaget (/opt/spark/jobs/).

Aktivering av streaming-IDPer:
  kubectl apply -f k8s/spark/folkeregister_person_idp.yaml
  kubectl apply -f k8s/spark/folkeregister_family_idp.yaml
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

SPARK_NS   = "slettix-analytics"
PORTAL_URL = os.environ.get("PORTAL_URL", "http://dataportal.slettix-analytics.svc.cluster.local:8090")
PORTAL_API_KEY = os.environ.get("PORTAL_API_KEY", "dev-key-change-me")

# ── SparkApplication YAML-maler ────────────────────────────────────────────

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


_PERSON_REGISTRY_APP   = _spark_app("fr-person-registry",  "folkeregister_person_registry.py")
_FAMILY_RELATIONS_APP  = _spark_app("fr-family-relations",  "folkeregister_family_relations.py")
_RESIDENCE_HISTORY_APP = _spark_app("fr-residence-history", "folkeregister_residence_history.py")
_VALIDATE_QUALITY_APP  = _spark_app("fr-validate-quality",  "folkeregister_validate_quality.py")


# ── Referansedata: kommuner ────────────────────────────────────────────────

def load_kommuner_reference(**context):
    """
    Leser kommuner.csv fra jobs-ConfigMap og skriver til Delta-tabell
    s3://silver/reference/kommuner.  Kjøres som første task i Silver-DAGen
    slik at Spark-jobbene alltid har en oppdatert kommuneoppslags-tabell.
    """
    import pandas as pd
    from deltalake.writer import write_deltalake

    log = context["task_instance"].log

    minio_endpoint   = os.environ.get("MINIO_ENDPOINT",    "http://minio.slettix-analytics.svc.cluster.local:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY",  "admin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY",  "changeme")

    storage_options = {
        "AWS_ENDPOINT_URL":           minio_endpoint,
        "AWS_ACCESS_KEY_ID":          minio_access_key,
        "AWS_SECRET_ACCESS_KEY":      minio_secret_key,
        "AWS_ALLOW_HTTP":             "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    import pyarrow as pa

    csv_path = "/opt/airflow/jobs/kommuner.csv"
    df = pd.read_csv(csv_path, sep=";", dtype=str)
    df.columns = [c.lower() for c in df.columns]
    df["kommunenr"] = df["kommunenr"].str.zfill(4)

    write_deltalake(
        "s3://silver/reference/kommuner",
        pa.Table.from_pandas(df, preserve_index=False),
        mode="overwrite",
        storage_options=storage_options,
    )
    log.info(f"Lastet {len(df)} kommuner til s3://silver/reference/kommuner")


# ── Feilhåndtering ─────────────────────────────────────────────────────────

def on_task_failure(context):
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exc     = context.get("exception")
    log_url = context["task_instance"].log_url

    message = (
        f"Folkeregister Silver — task feilet\n"
        f"  DAG    : {dag_id}\n"
        f"  Task   : {task_id}\n"
        f"  Feil   : {exc}\n"
        f"  Logg   : {log_url}"
    )
    context["task_instance"].log.error(message)

    slack_url = Variable.get("slack_webhook_url", default_var=None)
    if slack_url:
        try:
            from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
            SlackWebhookHook(webhook_token=slack_url).send(text=message)
        except Exception as e:
            context["task_instance"].log.warning(f"Slack-varsling feilet: {e}")



# ── Registrer DAG-lenker ───────────────────────────────────────────────────

def register_dag_links(**context):
    import json
    import os
    import sys

    import requests

    dag_id  = context["dag"].dag_id
    log     = context["task_instance"].log
    headers = {"X-API-Key": PORTAL_API_KEY}

    MANIFEST_DIR = "/opt/airflow/jobs"
    sys.path.insert(0, MANIFEST_DIR)

    products = [
        "folkeregister.person_registry",
        "folkeregister.family_relations",
        "folkeregister.residence_history",
    ]
    for product_id in products:
        # 1. Registrer fullstendig manifest (inkl. column_lineage) i Delta-registeret
        manifest_path = os.path.join(MANIFEST_DIR, f"{product_id}.json")
        if os.path.exists(manifest_path):
            try:
                from registry import register
                with open(manifest_path) as f:
                    manifest = json.load(f)
                manifest["dag_id"] = dag_id
                register(manifest)
                log.info(f"Manifest registrert i Delta-registeret for '{product_id}'")
            except Exception as exc:
                log.warning(f"Kunne ikke registrere manifest for '{product_id}': {exc}")
        else:
            log.warning(f"Manifest-fil ikke funnet: {manifest_path} — hopper over register()")

        # 2. Patch dag_id i portalen via API
        try:
            resp = requests.patch(
                f"{PORTAL_URL}/api/products/{product_id}",
                json={"dag_id": dag_id},
                headers=headers,
                timeout=10,
            )
            resp.raise_for_status()
            log.info(f"Registrert dag_id='{dag_id}' på produkt '{product_id}' via API")
        except Exception as exc:
            log.warning(f"Kunne ikke registrere dag_id på '{product_id}': {exc}")


# ── DAG-definisjon ─────────────────────────────────────────────────────────

default_args = {
    "owner":                     "slettix",
    "retries":                   2,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure":          False,
    "email_on_retry":            False,
    "on_failure_callback":       on_task_failure,
}

with DAG(
    dag_id="folkeregister_silver",
    description="Folkeregister Silver: Bronze → person_registry, family_relations, residence_history",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 15),
    catchup=False,
    default_args=default_args,
    tags=["folkeregister", "silver", "scd2", "pii"],
    doc_md=__doc__,
) as dag:

    load_reference = PythonOperator(
        task_id="load_kommuner_reference",
        python_callable=load_kommuner_reference,
        execution_timeout=timedelta(minutes=5),
    )

    person_registry = SparkKubernetesOperator(
        task_id="build_person_registry",
        namespace=SPARK_NS,
        application_file=_PERSON_REGISTRY_APP,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=30),
    )

    family_relations = SparkKubernetesOperator(
        task_id="build_family_relations",
        namespace=SPARK_NS,
        application_file=_FAMILY_RELATIONS_APP,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=20),
    )

    residence_history = SparkKubernetesOperator(
        task_id="build_residence_history",
        namespace=SPARK_NS,
        application_file=_RESIDENCE_HISTORY_APP,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=20),
    )

    quality_check = SparkKubernetesOperator(
        task_id="validate_quality",
        namespace=SPARK_NS,
        application_file=_VALIDATE_QUALITY_APP,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=20),
    )

    register_links = PythonOperator(
        task_id="register_dag_links",
        python_callable=register_dag_links,
        execution_timeout=timedelta(minutes=5),
    )

    load_reference >> person_registry >> family_relations >> residence_history >> quality_check >> register_links
