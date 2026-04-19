"""
Airflow DAG — Kafka IDP: folkeregister_person_events_idp
Mal: kafka_idp
Domene: folkeregister
Eier: folkeregister-team
Topics: folkeregister.citizen.created,folkeregister.event.birth,folkeregister.citizen.died,folkeregister.event.relocation
Target: s3a://bronze/folkeregister/person_events
Auto-generert av Slettix Analytics Portal
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka.customermaster.svc.cluster.local:9092",
)
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "http://minio.slettix-analytics.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "changeme")
PORTAL_URL       = os.environ.get("PORTAL_URL",       "http://dataportal.slettix-analytics.svc.cluster.local:8090")
PORTAL_API_KEY   = os.environ.get("PORTAL_API_KEY",   "dev-key-change-me")

KAFKA_TOPICS    = "folkeregister.citizen.created,folkeregister.event.birth,folkeregister.citizen.died,folkeregister.event.relocation"
TARGET_PATH     = "s3a://bronze/folkeregister/person_events"
CHECKPOINT_PATH = "s3a://checkpoints/folkeregister_person_events_idp"
CONSUMER_GROUP  = "folkeregister-person-events-idp"
PRODUCT_ID      = "folkeregister.person_events"
PRODUCT_NAME    = "Folkeregister — Personhendelser (IDP)"
DOMAIN          = "folkeregister"
OWNER           = "folkeregister-team"
TRIGGER_SECONDS = 5
FRESHNESS_HOURS = 1
PARTITION_BY    = "event_type,event_date"

SPARK_APP_NAME  = "folkeregister-person-events-idp-streaming"
SPARK_NAMESPACE = "slettix-analytics"
SPARK_NS        = SPARK_NAMESPACE


def deploy_streaming_job(**context):
    log = context["task_instance"].log

    manifest = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": SPARK_APP_NAME,
            "namespace": SPARK_NAMESPACE,
            "labels": {
                "app": SPARK_APP_NAME,
                "app.kubernetes.io/part-of": "slettix-analytics",
                "app.kubernetes.io/component": "idp-streaming",
                "app.kubernetes.io/managed-by": "pipeline-builder",
            },
            "annotations": {
                "slettix.io/product-id": PRODUCT_ID,
                "slettix.io/kafka-topics": KAFKA_TOPICS,
            },
        },
        "spec": {
            "type": "Python",
            "mode": "cluster",
            "image": "slettix-analytics/spark:3.5.8",
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": "local:///opt/spark/jobs/kafka_to_bronze.py",
            "arguments": [
                "--topics",          KAFKA_TOPICS,
                "--target-path",     TARGET_PATH,
                "--checkpoint-path", CHECKPOINT_PATH,
                "--consumer-group",  CONSUMER_GROUP,
                "--trigger-seconds", str(TRIGGER_SECONDS),
                "--partition-by",    PARTITION_BY,
                "--starting-offsets","earliest",
            ],
            "sparkVersion": "3.5.8",
            "restartPolicy": {
                "type": "Always",
                "onFailureRetries": 10,
                "onFailureRetryInterval": 30,
                "onSubmissionFailureRetries": 5,
                "onSubmissionFailureRetryInterval": 20,
            },
            "sparkConf": {
                "spark.sql.extensions":                        "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog":             "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.hadoop.fs.s3a.endpoint":                MINIO_ENDPOINT,
                "spark.hadoop.fs.s3a.path.style.access":       "true",
                "spark.hadoop.fs.s3a.impl":                    "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.connection.ssl.enabled":  "false",
                "spark.hadoop.fs.s3a.aws.credentials.provider":"com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
                "spark.sql.shuffle.partitions":                "4",
                "spark.databricks.delta.schema.autoMerge.enabled": "true",
            },
            "driver": {
                "cores": 1,
                "memory": "1g",
                "memoryOverhead": "256m",
                "serviceAccount": "spark",
                "env": [
                    {"name": "KAFKA_BOOTSTRAP_SERVERS", "value": KAFKA_BOOTSTRAP_SERVERS},
                    {"name": "AWS_ACCESS_KEY_ID",
                     "valueFrom": {"secretKeyRef": {"name": "slettix-credentials", "key": "minio-root-user"}}},
                    {"name": "AWS_SECRET_ACCESS_KEY",
                     "valueFrom": {"secretKeyRef": {"name": "slettix-credentials", "key": "minio-root-password"}}},
                ],
            },
            "executor": {
                "instances": 1,
                "cores": 1,
                "memory": "1g",
                "memoryOverhead": "256m",
                "env": [
                    {"name": "KAFKA_BOOTSTRAP_SERVERS", "value": KAFKA_BOOTSTRAP_SERVERS},
                    {"name": "AWS_ACCESS_KEY_ID",
                     "valueFrom": {"secretKeyRef": {"name": "slettix-credentials", "key": "minio-root-user"}}},
                    {"name": "AWS_SECRET_ACCESS_KEY",
                     "valueFrom": {"secretKeyRef": {"name": "slettix-credentials", "key": "minio-root-password"}}},
                ],
            },
        },
    }

    try:
        from kubernetes import client as k8s, config as k8s_config
        try:
            k8s_config.load_incluster_config()
        except Exception:
            k8s_config.load_kube_config()

        custom_api = k8s.CustomObjectsApi()
        grp, ver, plural = "sparkoperator.k8s.io", "v1beta2", "sparkapplications"

        try:
            existing = custom_api.get_namespaced_custom_object(
                group=grp, version=ver, namespace=SPARK_NAMESPACE,
                plural=plural, name=SPARK_APP_NAME,
            )
            manifest["metadata"]["resourceVersion"] = existing["metadata"]["resourceVersion"]
            custom_api.replace_namespaced_custom_object(
                group=grp, version=ver, namespace=SPARK_NAMESPACE,
                plural=plural, name=SPARK_APP_NAME, body=manifest,
            )
            log.info(f"SparkApplication {SPARK_APP_NAME} oppdatert")
        except k8s.ApiException as exc:
            if exc.status == 404:
                custom_api.create_namespaced_custom_object(
                    group=grp, version=ver, namespace=SPARK_NAMESPACE,
                    plural=plural, body=manifest,
                )
                log.info(f"SparkApplication {SPARK_APP_NAME} opprettet")
            else:
                raise
    except Exception as exc:
        log.error(f"Kunne ikke deploye SparkApplication: {exc}")
        raise


def register_product(**context):
    import json
    log = context["task_instance"].log
    sys.path.insert(0, "/opt/airflow/jobs")

    manifest = {
        "id":           PRODUCT_ID,
        "name":         PRODUCT_NAME,
        "domain":       DOMAIN,
        "owner":        OWNER,
        "version":      "1.0.0",
        "description":  "Kildenær IDP som konsumerer personhendelser fra Folkeregisteret via Kafka. "
                        "CloudEvent-konvolutten pakkes ut; payload lagres som payload_json for transformasjon i Silver.",
        "source_path":  TARGET_PATH.replace("s3a://", "s3://"),
        "format":       "delta",
        "dag_id":       "folkeregister_person_events_idp",
        "product_type": "source",
        "access":       "restricted",
        "quality_sla":  {"freshness_hours": FRESHNESS_HOURS},
        "tags":         ["kafka", "streaming", "idp", "bronze", DOMAIN],
        "contract": {
            "slo": {"freshness_hours": FRESHNESS_HOURS},
            "schema_compatibility": "FORWARD",
        },
        "schema": [
            {"name": "event_id",        "type": "string",    "nullable": True,  "pii": False, "description": "CloudEvent eventId"},
            {"name": "event_type",      "type": "string",    "nullable": True,  "pii": False, "description": "CloudEvent eventType"},
            {"name": "event_timestamp", "type": "string",    "nullable": True,  "pii": False, "description": "CloudEvent occurredAt"},
            {"name": "event_source",    "type": "string",    "nullable": True,  "pii": False, "description": "CloudEvent source"},
            {"name": "event_version",   "type": "string",    "nullable": True,  "pii": False, "description": "CloudEvent version"},
            {"name": "payload_json",    "type": "string",    "nullable": True,  "pii": True,  "sensitivity": "high",
             "description": "Rå JSON-payload — domenespesifikke felt transformeres i Silver"},
            {"name": "kafka_topic",     "type": "string",    "nullable": False, "pii": False, "description": "Kafka-topic meldingen ble lest fra"},
            {"name": "kafka_partition", "type": "integer",   "nullable": False, "pii": False},
            {"name": "kafka_offset",    "type": "long",      "nullable": False, "pii": False},
            {"name": "kafka_timestamp", "type": "timestamp", "nullable": True,  "pii": False},
            {"name": "_processed_at",   "type": "timestamp", "nullable": False, "pii": False, "description": "Prosesseringstidspunkt (IDP)"},
            {"name": "_raw_value",      "type": "string",    "nullable": True,  "pii": True,  "sensitivity": "high",
             "description": "Rå Kafka-meldingsverdi (JSON-streng)"},
            {"name": "event_date",      "type": "date",      "nullable": True,  "pii": False, "description": "Partisjoneringskolonne (avledet av event_timestamp)"},
        ],
    }

    try:
        from registry import register
        register(manifest)
        log.info(f"Produkt {PRODUCT_ID} registrert i Delta-registeret")
    except Exception as exc:
        log.warning(f"Registrering i Delta-registeret feilet: {exc}")

    try:
        import requests
        requests.patch(
            f"{PORTAL_URL}/api/products/{PRODUCT_ID}",
            json={"dag_id": "folkeregister_person_events_idp"},
            headers={"X-API-Key": PORTAL_API_KEY},
            timeout=10,
        )
        log.info(f"dag_id patchet i portalen for {PRODUCT_ID}")
    except Exception as exc:
        log.warning(f"Kunne ikke patche dag_id i portalen: {exc}")


default_args = {
    "owner":       "folkeregister-team",
    "retries":     1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="folkeregister_person_events_idp",
    description="Kafka IDP: folkeregister person-hendelser → Bronze",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["generated", "kafka", "streaming", "idp", "folkeregister"],
    doc_md=__doc__,
) as dag:

    deploy = PythonOperator(
        task_id="deploy_streaming_job",
        python_callable=deploy_streaming_job,
        execution_timeout=timedelta(minutes=5),
    )

    register = PythonOperator(
        task_id="register_product",
        python_callable=register_product,
        execution_timeout=timedelta(minutes=2),
    )

    deploy >> register
