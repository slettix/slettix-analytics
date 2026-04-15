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
from kubernetes import client as k8s

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
    memory: "512m"
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


# ── Datakvalitetsvalidering ────────────────────────────────────────────────

def validate_quality(**context):
    """
    Les de tre Silver-tabellene med deltalake og kjør enkle Great Expectations-sjekker.
    Resultatene skrives til MinIO: quality_results/{product_id}/latest.json
    """
    import json
    from datetime import datetime, timezone

    import boto3
    import great_expectations as gx
    from botocore.client import Config
    from deltalake import DeltaTable

    log = context["task_instance"].log

    minio_endpoint   = os.environ.get("MINIO_ENDPOINT", "http://minio.slettix-analytics.svc.cluster.local:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "admin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "changeme")

    storage_options = {
        "AWS_ENDPOINT_URL":           minio_endpoint,
        "AWS_ACCESS_KEY_ID":          minio_access_key,
        "AWS_SECRET_ACCESS_KEY":      minio_secret_key,
        "AWS_ALLOW_HTTP":             "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=Config(signature_version="s3v4"),
    )

    checks = [
        {
            "product_id": "folkeregister.person_registry",
            "path":       "s3://silver/folkeregister/person_registry",
            "expectations": [
                ("expect_column_values_to_not_be_null", {"column": "person_id"}),
                ("expect_column_values_to_not_be_null", {"column": "full_name"}),
                ("expect_column_values_to_not_be_null", {"column": "fnr_masked"}),
                ("expect_column_to_exist",              {"column": "marital_status"}),
                ("expect_table_row_count_to_be_between", {"min_value": 1, "max_value": None}),
            ],
        },
        {
            "product_id": "folkeregister.family_relations",
            "path":       "s3://silver/folkeregister/family_relations",
            "expectations": [
                ("expect_column_values_to_not_be_null", {"column": "person_id_a"}),
                ("expect_column_values_to_not_be_null", {"column": "person_id_b"}),
                ("expect_table_row_count_to_be_between", {"min_value": 1, "max_value": None}),
            ],
        },
        {
            "product_id": "folkeregister.residence_history",
            "path":       "s3://silver/folkeregister/residence_history",
            "expectations": [
                ("expect_column_values_to_not_be_null", {"column": "person_id"}),
                ("expect_table_row_count_to_be_between", {"min_value": 1, "max_value": None}),
            ],
        },
    ]

    context_gx = gx.get_context(mode="ephemeral")

    for check in checks:
        product_id = check["product_id"]
        log.info(f"Validerer {product_id} …")
        try:
            dt  = DeltaTable(check["path"], storage_options=storage_options)
            df  = dt.to_pandas()

            ds  = context_gx.data_sources.add_pandas("ds_" + product_id.replace(".", "_"))
            da  = ds.add_dataframe_asset("asset")
            bd  = da.add_batch_definition_whole_dataframe("batch")
            suite = context_gx.suites.add(gx.ExpectationSuite(name="suite_" + product_id.replace(".", "_")))
            for exp_type, kwargs in check["expectations"]:
                suite.add_expectation(gx.expectations.__dict__[
                    "".join(p.capitalize() for p in exp_type.split("_"))
                ](**kwargs))

            val_def = context_gx.validation_definitions.add(
                gx.ValidationDefinition(name="val_" + product_id.replace(".", "_"), data=bd, suite=suite)
            )
            result = val_def.run(batch_parameters={"dataframe": df})

            passed = sum(1 for r in result.results if r.success)
            failed = [r for r in result.results if not r.success]
            quality_result = {
                "product_id":         product_id,
                "validated_at":       datetime.now(tz=timezone.utc).isoformat(),
                "score_pct":          round(passed / len(result.results) * 100, 1),
                "total_expectations": len(result.results),
                "passed":             passed,
                "failed":             len(failed),
                "failures": [
                    {"expectation": r.expectation_config.type, "kwargs": r.expectation_config.kwargs}
                    for r in failed
                ],
            }
        except Exception as exc:
            log.warning(f"Validering av {product_id} feilet: {exc}")
            quality_result = {
                "product_id":         product_id,
                "validated_at":       datetime.now(tz=timezone.utc).isoformat(),
                "score_pct":          0.0,
                "total_expectations": 0,
                "passed":             0,
                "failed":             1,
                "failures":           [{"expectation": "pipeline", "error": str(exc)}],
            }

        ts_key = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
        for key in [
            f"quality_results/{product_id}/latest.json",
            f"quality_results/{product_id}/history/{ts_key}.json",
        ]:
            try:
                s3.put_object(
                    Bucket="gold", Key=key,
                    Body=json.dumps(quality_result, indent=2).encode(),
                    ContentType="application/json",
                )
            except Exception as exc:
                log.warning(f"Kunne ikke lagre kvalitetsresultat for {product_id}: {exc}")

        log.info(f"{product_id}: {quality_result['score_pct']}% ({quality_result['passed']}/{quality_result['total_expectations']})")


# ── Registrer DAG-lenker ───────────────────────────────────────────────────

def register_dag_links(**context):
    import requests

    dag_id  = context["dag"].dag_id
    log     = context["task_instance"].log
    headers = {"X-API-Key": PORTAL_API_KEY}

    products = [
        "folkeregister.person_registry",
        "folkeregister.family_relations",
        "folkeregister.residence_history",
    ]
    for product_id in products:
        try:
            resp = requests.patch(
                f"{PORTAL_URL}/api/products/{product_id}",
                json={"dag_id": dag_id},
                headers=headers,
                timeout=10,
            )
            resp.raise_for_status()
            log.info(f"Registrert dag_id='{dag_id}' på produkt '{product_id}'")
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

    quality_check = PythonOperator(
        task_id="validate_quality",
        python_callable=validate_quality,
        execution_timeout=timedelta(minutes=15),
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            resources=k8s.V1ResourceRequirements(
                                requests={"memory": "512Mi"},
                                limits={"memory": "1Gi"},
                            ),
                        )
                    ]
                )
            )
        },
    )

    register_links = PythonOperator(
        task_id="register_dag_links",
        python_callable=register_dag_links,
        execution_timeout=timedelta(minutes=5),
    )

    person_registry >> family_relations >> residence_history >> quality_check >> register_links
