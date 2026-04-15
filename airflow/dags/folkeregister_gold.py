"""
Folkeregister Gold DAG

Bygger fem aggregerte, PII-frie Gold-dataprodukter fra Silver-laget:

  population_stats       — Befolkningsstatistikk per kommune og år        (C-1)
  cohort_demographics    — Demografiprofil per fødselsdekade og kjønn      (C-2)
  marital_status_trends  — Sivilstandsfordeling over tid og aldersgruppe   (C-3)
  migration_flows        — Migrasjonsstrømmer mellom kommunepar             (C-4)
  household_structure    — Familiestørrelser og husstandstyper              (C-5)

Kjørerekke:
  population_stats ──┐
  cohort_demographics ┤
  marital_status_trends ┤──► validate_quality ──► register_dag_links
  migration_flows     ┤
  household_structure ─┘

  C-1 til C-5 kjøres parallelt (ingen innbyrdes avhengigheter).
  validate_quality og register_dag_links kjøres etter alle fem.

Avhengigheter:
  - folkeregister_silver DAG: Silver-tabellene må eksistere

SparkKubernetesOperator starter SparkApplication-CRD-er i slettix-analytics
namespace.  Jobbene er montert inn fra ConfigMap airflow-jobs.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from kubernetes import client as k8s

SPARK_NS       = "slettix-analytics"
PORTAL_URL     = os.environ.get("PORTAL_URL",     "http://dataportal.slettix-analytics.svc.cluster.local:8090")
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


_POPULATION_STATS_APP      = _spark_app("fr-population-stats",      "folkeregister_population_stats.py")
_COHORT_DEMOGRAPHICS_APP   = _spark_app("fr-cohort-demographics",   "folkeregister_cohort_demographics.py")
_MARITAL_TRENDS_APP        = _spark_app("fr-marital-status-trends",  "folkeregister_marital_status_trends.py")
_MIGRATION_FLOWS_APP       = _spark_app("fr-migration-flows",       "folkeregister_migration_flows.py")
_HOUSEHOLD_STRUCTURE_APP   = _spark_app("fr-household-structure",   "folkeregister_household_structure.py")


# ── Feilhåndtering ─────────────────────────────────────────────────────────

def on_task_failure(context):
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exc     = context.get("exception")
    log_url = context["task_instance"].log_url

    message = (
        f"Folkeregister Gold — task feilet\n"
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
    Kjør Great Expectations-sjekker på alle fem Gold-tabeller.
    Resultater skrives til MinIO: quality_results/{product_id}/latest.json
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
            "product_id": "folkeregister.population_stats",
            "path":       "s3://gold/folkeregister/population_stats",
            "expectations": [
                ("expect_column_values_to_not_be_null", {"column": "municipality_code"}),
                ("expect_column_values_to_not_be_null", {"column": "reference_year"}),
                ("expect_column_values_to_not_be_null", {"column": "population_total"}),
                ("expect_column_values_to_be_between",  {"column": "population_total", "min_value": 0}),
                ("expect_table_row_count_to_be_between", {"min_value": 1, "max_value": None}),
            ],
        },
        {
            "product_id": "folkeregister.cohort_demographics",
            "path":       "s3://gold/folkeregister/cohort_demographics",
            "expectations": [
                ("expect_column_values_to_not_be_null", {"column": "birth_decade"}),
                ("expect_column_values_to_not_be_null", {"column": "cohort_size"}),
                ("expect_column_values_to_be_between",  {"column": "survival_rate_pct", "min_value": 0, "max_value": 100}),
                ("expect_table_row_count_to_be_between", {"min_value": 1, "max_value": None}),
            ],
        },
        {
            "product_id": "folkeregister.marital_status_trends",
            "path":       "s3://gold/folkeregister/marital_status_trends",
            "expectations": [
                ("expect_column_values_to_not_be_null", {"column": "reference_year"}),
                ("expect_column_values_to_not_be_null", {"column": "age_group"}),
                ("expect_column_values_to_not_be_null", {"column": "marital_status"}),
                ("expect_column_values_to_be_between",  {"column": "pct_of_age_group", "min_value": 0, "max_value": 100}),
                ("expect_table_row_count_to_be_between", {"min_value": 1, "max_value": None}),
            ],
        },
        {
            "product_id": "folkeregister.migration_flows",
            "path":       "s3://gold/folkeregister/migration_flows",
            "expectations": [
                ("expect_column_values_to_not_be_null", {"column": "from_municipality_code"}),
                ("expect_column_values_to_not_be_null", {"column": "to_municipality_code"}),
                ("expect_column_values_to_not_be_null", {"column": "flow_count"}),
                ("expect_table_row_count_to_be_between", {"min_value": 1, "max_value": None}),
            ],
        },
        {
            "product_id": "folkeregister.household_structure",
            "path":       "s3://gold/folkeregister/household_structure",
            "expectations": [
                ("expect_column_values_to_not_be_null", {"column": "household_type"}),
                ("expect_column_values_to_not_be_null", {"column": "count"}),
                ("expect_column_values_to_be_between",  {"column": "pct_of_total", "min_value": 0, "max_value": 100}),
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

            ds    = context_gx.data_sources.add_pandas("ds_" + product_id.replace(".", "_"))
            da    = ds.add_dataframe_asset("asset")
            bd    = da.add_batch_definition_whole_dataframe("batch")
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
        "folkeregister.population_stats",
        "folkeregister.cohort_demographics",
        "folkeregister.marital_status_trends",
        "folkeregister.migration_flows",
        "folkeregister.household_structure",
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
    dag_id="folkeregister_gold",
    description="Folkeregister Gold: Silver → population_stats, cohort_demographics, marital_status_trends, migration_flows, household_structure",
    schedule="0 4 * * *",          # kjøres 2 timer etter Silver-DAGen (02:00)
    start_date=datetime(2024, 1, 15),
    catchup=False,
    default_args=default_args,
    tags=["folkeregister", "gold", "aggregat", "public"],
    doc_md=__doc__,
) as dag:

    population_stats = SparkKubernetesOperator(
        task_id="build_population_stats",
        namespace=SPARK_NS,
        application_file=_POPULATION_STATS_APP,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=30),
    )

    cohort_demographics = SparkKubernetesOperator(
        task_id="build_cohort_demographics",
        namespace=SPARK_NS,
        application_file=_COHORT_DEMOGRAPHICS_APP,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=20),
    )

    marital_status_trends = SparkKubernetesOperator(
        task_id="build_marital_status_trends",
        namespace=SPARK_NS,
        application_file=_MARITAL_TRENDS_APP,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=20),
    )

    migration_flows = SparkKubernetesOperator(
        task_id="build_migration_flows",
        namespace=SPARK_NS,
        application_file=_MIGRATION_FLOWS_APP,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=20),
    )

    household_structure = SparkKubernetesOperator(
        task_id="build_household_structure",
        namespace=SPARK_NS,
        application_file=_HOUSEHOLD_STRUCTURE_APP,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=20),
    )

    quality_check = PythonOperator(
        task_id="validate_quality",
        python_callable=validate_quality,
        execution_timeout=timedelta(minutes=20),
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

    # C-1 til C-5 kjøres parallelt; deretter kvalitetssjekk og registrering
    [
        population_stats,
        cohort_demographics,
        marital_status_trends,
        migration_flows,
        household_structure,
    ] >> quality_check >> register_links
