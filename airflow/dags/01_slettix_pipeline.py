"""
Slettix Analytics — end-to-end pipeline DAG

Kjørerekke:
  ingest_employees_to_bronze
      → bronze_to_silver_employees
          → validate_silver_employees   (Great Expectations)
              → build_gold_department_stats
                  → validate_gold_department_stats
                      → detect_anomalies
                          → register_dag_link
                              → pipeline_complete

Egenskaper:
- Kjøres daglig @daily, kan også trigges manuelt fra Airflow UI
- Spark-jobber startes via SparkKubernetesOperator (SparkApplication CRD)
- Python-tasks kjøres i Airflow task-pod med env vars fra slettix-credentials Secret
- Feil i ett steg avbryter alle nedstrøms steg (Airflows standard oppførsel)
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

SPARK_NS = "slettix-analytics"
MINIO_ENDPOINT = "http://minio.slettix-analytics.svc.cluster.local:9000"


# ── SparkApplication YAML-maler (rendres av Airflow Jinja ved kjøring) ────────

_INGEST_BRONZE_APP = """
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: ingest-bronze-{{ ds_nodash }}
  namespace: slettix-analytics
spec:
  type: Python
  mode: cluster
  image: slettix-analytics/spark:3.5.8
  imagePullPolicy: IfNotPresent
  mainApplicationFile: "local:///opt/spark/jobs/ingest_to_bronze.py"
  arguments:
    - "--source"
    - "s3a://raw/hr/employees"
    - "--target"
    - "s3a://bronze/hr/employees"
    - "--format"
    - "csv"
    - "--ingestion-date"
    - "{{ ds }}"
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
  executor:
    instances: 1
    cores: 1
    memory: "1g"
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
"""

_BRONZE_TO_SILVER_APP = """
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: bronze-to-silver-{{ ds_nodash }}
  namespace: slettix-analytics
spec:
  type: Python
  mode: cluster
  image: slettix-analytics/spark:3.5.8
  imagePullPolicy: IfNotPresent
  mainApplicationFile: "local:///opt/spark/jobs/transform_bronze_to_silver.py"
  arguments:
    - "--config"
    - "/opt/spark/conf/silver/hr/employees.json"
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
  executor:
    instances: 1
    cores: 1
    memory: "1g"
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
"""

_BUILD_GOLD_APP = """
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: build-gold-{{ ds_nodash }}
  namespace: slettix-analytics
spec:
  type: Python
  mode: cluster
  image: slettix-analytics/spark:3.5.8
  imagePullPolicy: IfNotPresent
  mainApplicationFile: "local:///opt/spark/jobs/build_gold_table.py"
  arguments:
    - "--config"
    - "/opt/spark/conf/gold/hr/department_stats.json"
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
  executor:
    instances: 1
    cores: 1
    memory: "1g"
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
"""


# ── Feilhåndtering ─────────────────────────────────────────────────────────

def on_task_failure(context):
    dag_id   = context["dag"].dag_id
    task_id  = context["task_instance"].task_id
    run_id   = context["run_id"]
    exc      = context.get("exception")
    log_url  = context["task_instance"].log_url

    message = (
        f"Pipeline-feil i Slettix Analytics\n"
        f"  DAG    : {dag_id}\n"
        f"  Task   : {task_id}\n"
        f"  Run ID : {run_id}\n"
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


# ── Python-tasks (kjøres i Airflow task-pod — env vars fra K8s Secret) ────────

def validate_silver_employees(**context):
    """
    Les Silver-tabellen med delta-rs (ingen Spark nødvendig) og kjør
    Great Expectations-validering. Kaster AirflowException ved brudd.
    """
    import json
    from datetime import datetime, timezone

    import boto3
    import great_expectations as gx
    from airflow.exceptions import AirflowException
    from botocore.client import Config
    from deltalake import DeltaTable

    log = context["task_instance"].log

    minio_endpoint   = os.environ.get("MINIO_ENDPOINT", MINIO_ENDPOINT)
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "admin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "changeme")

    storage_options = {
        "AWS_ENDPOINT_URL":           minio_endpoint,
        "AWS_ACCESS_KEY_ID":          minio_access_key,
        "AWS_SECRET_ACCESS_KEY":      minio_secret_key,
        "AWS_ALLOW_HTTP":             "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    log.info("Leser Silver/employees via delta-rs ...")
    dt = DeltaTable("s3://silver/hr/employees", storage_options=storage_options)
    df = dt.to_pandas()
    log.info(f"Leste {len(df)} rader fra Silver.")

    gx_ctx = gx.get_context(mode="ephemeral")
    ds         = gx_ctx.data_sources.add_pandas("silver_source")
    asset      = ds.add_dataframe_asset("employees_asset")
    batch_def  = asset.add_batch_definition_whole_dataframe("full_batch")

    suite = gx_ctx.suites.add(gx.ExpectationSuite(name="silver_employees"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="name"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="department"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="salary", min_value=0, max_value=500_000,
    ))

    val_def = gx_ctx.validation_definitions.add(
        gx.ValidationDefinition(
            name="validate_silver_employees",
            data=batch_def,
            suite=suite,
        )
    )

    result = val_def.run(batch_parameters={"dataframe": df})

    passed = sum(1 for r in result.results if r.success)
    failed = [r for r in result.results if not r.success]
    log.info(f"GE-validering: {passed}/{len(result.results)} forventninger bestått.")

    quality_result = {
        "product_id":         "hr.employees",
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

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            config=Config(signature_version="s3v4"),
        )
        ts_key = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
        for key in [
            "quality_results/hr.employees/latest.json",
            f"quality_results/hr.employees/history/{ts_key}.json",
        ]:
            s3.put_object(
                Bucket="gold", Key=key,
                Body=json.dumps(quality_result, indent=2).encode(),
                ContentType="application/json",
            )
        log.info("GE-resultat lagret til MinIO")
    except Exception as exc:
        log.warning(f"Kunne ikke lagre GE-resultat: {exc}")

    if not result.success:
        failures_str = "\n".join(
            f"  - {r.expectation_config.type} ({r.expectation_config.kwargs})"
            for r in failed
        )
        raise AirflowException(f"Silver-validering feilet — {len(failed)} brudd:\n{failures_str}")

    log.info("Silver-validering OK.")


def validate_gold_department_stats(**context):
    """Valider Gold-tabellen hr.department_stats med Great Expectations."""
    import json
    from datetime import datetime, timezone

    import boto3
    import great_expectations as gx
    from airflow.exceptions import AirflowException
    from botocore.client import Config
    from deltalake import DeltaTable

    log = context["task_instance"].log

    minio_endpoint   = os.environ.get("MINIO_ENDPOINT", MINIO_ENDPOINT)
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "admin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "changeme")

    storage_options = {
        "AWS_ENDPOINT_URL":           minio_endpoint,
        "AWS_ACCESS_KEY_ID":          minio_access_key,
        "AWS_SECRET_ACCESS_KEY":      minio_secret_key,
        "AWS_ALLOW_HTTP":             "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    log.info("Leser Gold/department_stats via delta-rs ...")
    dt = DeltaTable("s3://gold/hr/department_stats", storage_options=storage_options)
    df = dt.to_pandas()
    log.info(f"Leste {len(df)} rader fra Gold.")

    gx_ctx    = gx.get_context(mode="ephemeral")
    ds        = gx_ctx.data_sources.add_pandas("gold_source")
    asset     = ds.add_dataframe_asset("department_stats_asset")
    batch_def = asset.add_batch_definition_whole_dataframe("full_batch")

    suite = gx_ctx.suites.add(gx.ExpectationSuite(name="gold_department_stats"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="department"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="department"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="headcount"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="headcount", min_value=1))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="avg_salary"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="avg_salary", min_value=0))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="total_payroll"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="total_payroll", min_value=0))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="min_salary", min_value=0))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="max_salary", min_value=0))

    val_def = gx_ctx.validation_definitions.add(
        gx.ValidationDefinition(
            name="validate_gold_department_stats",
            data=batch_def,
            suite=suite,
        )
    )

    result = val_def.run(batch_parameters={"dataframe": df})

    passed = sum(1 for r in result.results if r.success)
    failed = [r for r in result.results if not r.success]
    log.info(f"GE-validering Gold: {passed}/{len(result.results)} forventninger bestått.")

    quality_result = {
        "product_id":         "hr.department_stats",
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

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            config=Config(signature_version="s3v4"),
        )
        ts_key = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
        for key in [
            "quality_results/hr.department_stats/latest.json",
            f"quality_results/hr.department_stats/history/{ts_key}.json",
        ]:
            s3.put_object(
                Bucket="gold", Key=key,
                Body=json.dumps(quality_result, indent=2).encode(),
                ContentType="application/json",
            )
        log.info("GE-resultat lagret til MinIO")
    except Exception as exc:
        log.warning(f"Kunne ikke lagre GE-resultat: {exc}")

    if not result.success:
        failures_str = "\n".join(
            f"  - {r.expectation_config.type} ({r.expectation_config.kwargs})"
            for r in failed
        )
        raise AirflowException(f"Gold-validering feilet — {len(failed)} brudd:\n{failures_str}")

    log.info("Gold-validering OK.")


def detect_anomalies(**context):
    """Les kvalitetshistorikk og oppdag statistiske avvik. Oppretter incidents for kritiske funn."""
    import json
    from datetime import datetime, timezone

    import boto3
    import requests
    from botocore.client import Config

    log        = context["task_instance"].log
    portal_url = os.environ.get("PORTAL_URL", "http://dataportal.slettix-analytics.svc.cluster.local:8090")
    api_key    = os.environ.get("PORTAL_API_KEY", "dev-key-change-me")

    minio_endpoint   = os.environ.get("MINIO_ENDPOINT", MINIO_ENDPOINT)
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "admin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "changeme")

    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=Config(signature_version="s3v4"),
    )

    for product_id in ["hr.employees", "hr.department_stats"]:
        log.info(f"[anomaly] Sjekker {product_id} ...")
        anomalies = []

        try:
            resp = s3.list_objects_v2(
                Bucket="gold",
                Prefix=f"quality_results/{product_id}/history/",
            )
            history_keys = sorted(
                [o["Key"] for o in resp.get("Contents", [])],
                reverse=True,
            )[:30]
        except Exception as exc:
            log.warning(f"[anomaly] Kunne ikke lese historikk for {product_id}: {exc}")
            continue

        if len(history_keys) < 2:
            log.info(f"[anomaly] For lite historikk for {product_id}, hopper over")
            continue

        history = []
        for key in history_keys:
            try:
                obj = s3.get_object(Bucket="gold", Key=key)
                history.append(json.loads(obj["Body"].read()))
            except Exception:
                pass

        if not history:
            continue

        latest = history[0]
        prev   = history[1] if len(history) > 1 else None

        if prev and (prev["score_pct"] - latest["score_pct"]) > 20:
            drop = prev["score_pct"] - latest["score_pct"]
            anomalies.append({
                "type":     "score_drop",
                "message":  f"Kvalitetsscore falt {drop:.1f} pp (fra {prev['score_pct']}% til {latest['score_pct']}%)",
                "severity": "critical" if drop > 40 else "warning",
            })

        if latest["score_pct"] < 80:
            anomalies.append({
                "type":     "low_score",
                "message":  f"Kvalitetsscore er {latest['score_pct']}% — under 80%-terskelen",
                "severity": "critical" if latest["score_pct"] < 60 else "warning",
            })

        if len(history) >= 3:
            all_failures = [
                set(f["expectation"] for f in run.get("failures", []))
                for run in history[:3]
            ]
            if all(all_failures):
                for exp in all_failures[0].intersection(*all_failures[1:]):
                    anomalies.append({
                        "type":     "repeated_failure",
                        "message":  f"Forventning '{exp}' har feilet de siste 3 kjøringene på rad",
                        "severity": "warning",
                    })

        anomaly_result = {
            "product_id":  product_id,
            "checked_at":  datetime.now(tz=timezone.utc).isoformat(),
            "anomalies":   anomalies,
            "has_anomaly": len(anomalies) > 0,
        }

        try:
            s3.put_object(
                Bucket="gold",
                Key=f"quality_results/{product_id}/anomalies/latest.json",
                Body=json.dumps(anomaly_result, indent=2).encode(),
                ContentType="application/json",
            )
        except Exception as exc:
            log.warning(f"[anomaly] Kunne ikke lagre anomalier for {product_id}: {exc}")

        headers = {"X-API-Key": api_key}
        for a in anomalies:
            if a["severity"] == "critical":
                try:
                    requests.post(
                        f"{portal_url}/api/products/{product_id}/incidents",
                        json={
                            "title":       a["message"][:120],
                            "description": f"Automatisk oppdaget av anomalideteksjon. Type: {a['type']}",
                            "severity":    a["severity"],
                        },
                        headers=headers,
                        timeout=10,
                    )
                except Exception as exc:
                    log.warning(f"[anomaly] Kunne ikke opprette incident: {exc}")


def register_dag_link(**context):
    """Patcher dag_id inn i produktmanifestene i portalen (idempotent)."""
    import requests

    portal_url = os.environ.get("PORTAL_URL", "http://dataportal.slettix-analytics.svc.cluster.local:8090")
    api_key    = os.environ.get("PORTAL_API_KEY", "dev-key-change-me")
    dag_id     = context["dag"].dag_id
    log        = context["task_instance"].log

    for product_id in ["hr.employees", "hr.department_stats"]:
        try:
            resp = requests.patch(
                f"{portal_url}/api/products/{product_id}",
                json={"dag_id": dag_id},
                headers={"X-API-Key": api_key},
                timeout=10,
            )
            resp.raise_for_status()
            log.info(f"Registrert dag_id='{dag_id}' på produkt '{product_id}'")
        except Exception as exc:
            log.warning(f"Kunne ikke registrere dag_id på '{product_id}': {exc}")


def log_pipeline_success(**context):
    run_id    = context["run_id"]
    exec_date = context["ds"]
    context["task_instance"].log.info(
        f"Slettix Analytics pipeline fullført\n"
        f"  Exec date : {exec_date}\n"
        f"  Run ID    : {run_id}\n"
        f"  Kjørerekke: ingest → bronze → silver → validate → gold → anomaly → register"
    )


# ── DAG-definisjon ─────────────────────────────────────────────────────────

default_args = {
    "owner":                     "slettix",
    "retries":                   2,
    "retry_delay":               timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "email_on_failure":          False,
    "email_on_retry":            False,
    "on_failure_callback":       on_task_failure,
}

with DAG(
    dag_id="01_slettix_pipeline",
    description="End-to-end pipeline: Ingest → Bronze → Silver → Gold (SparkKubernetesOperator)",
    schedule="@daily",
    start_date=datetime(2024, 1, 15),
    catchup=False,
    default_args=default_args,
    tags=["slettix", "medallion"],
    doc_md=__doc__,
) as dag:

    ingest = SparkKubernetesOperator(
        task_id="ingest_employees_to_bronze",
        namespace=SPARK_NS,
        application_file=_INGEST_BRONZE_APP,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=20),
    )

    bronze_to_silver = SparkKubernetesOperator(
        task_id="bronze_to_silver_employees",
        namespace=SPARK_NS,
        application_file=_BRONZE_TO_SILVER_APP,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=20),
    )

    validate_silver = PythonOperator(
        task_id="validate_silver_employees",
        python_callable=validate_silver_employees,
        execution_timeout=timedelta(minutes=10),
    )

    build_gold = SparkKubernetesOperator(
        task_id="build_gold_department_stats",
        namespace=SPARK_NS,
        application_file=_BUILD_GOLD_APP,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=20),
    )

    validate_gold = PythonOperator(
        task_id="validate_gold_department_stats",
        python_callable=validate_gold_department_stats,
        execution_timeout=timedelta(minutes=10),
    )

    detect_anomaly = PythonOperator(
        task_id="detect_anomalies",
        python_callable=detect_anomalies,
        execution_timeout=timedelta(minutes=10),
    )

    register_link = PythonOperator(
        task_id="register_dag_link",
        python_callable=register_dag_link,
    )

    complete = PythonOperator(
        task_id="pipeline_complete",
        python_callable=log_pipeline_success,
    )

    ingest >> bronze_to_silver >> validate_silver >> build_gold >> validate_gold >> detect_anomaly >> register_link >> complete
