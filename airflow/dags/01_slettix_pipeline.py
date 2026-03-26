"""
Slettix Analytics — end-to-end pipeline DAG

Kjørerekke:
  ingest_employees_to_bronze
      → bronze_to_silver_employees
          → build_gold_department_stats
              → pipeline_complete  (log-oppgave)

Egenskaper:
- Kjøres daglig @daily, kan også trigges manuelt fra Airflow UI
- Feil i ett steg avbryter alle nedstrøms steg (Airflows standard oppførsel)
- on_failure_callback logger feilinfo og kan utvides med Slack/e-post
- Varsling via e-post kan aktiveres ved å sette AIRFLOW__SMTP__* env-vars
  og email_on_failure=True i default_args

Konfigurasjon:
- Slack-varsling: opprett en Airflow-variabel 'slack_webhook_url' i Airflow UI
- E-postvarsling: sett AIRFLOW__SMTP__SMTP_HOST, ..._PORT, ..._USER, ..._PASSWORD
  og endre email_on_failure til True nedenfor
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_ID = "spark_default"

SPARK_CONF = {
    "spark.sql.extensions":                       "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog":            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.endpoint":               "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key":             "admin",
    "spark.hadoop.fs.s3a.secret.key":             "changeme",
    "spark.hadoop.fs.s3a.path.style.access":      "true",
    "spark.hadoop.fs.s3a.impl":                   "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.sql.shuffle.partitions":               "8",
    "spark.driver.host":                          "airflow-scheduler",
    "spark.driver.bindAddress":                   "0.0.0.0",
}

DELTA_JARS = ",".join([
    "io.delta:delta-spark_2.12:3.2.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])


# ── Feilhåndtering ─────────────────────────────────────────────────────────

def on_task_failure(context):
    """
    Kalles automatisk av Airflow når en task feiler.

    Logger feilinfo og kan utvides med:
    - Slack: bruk SlackWebhookHook med Airflow-variabelen 'slack_webhook_url'
    - E-post: sett email_on_failure=True og konfigurer SMTP
    """
    dag_id   = context["dag"].dag_id
    task_id  = context["task_instance"].task_id
    run_id   = context["run_id"]
    exc      = context.get("exception")
    log_url  = context["task_instance"].log_url

    message = (
        f"❌ Pipeline-feil i Slettix Analytics\n"
        f"  DAG    : {dag_id}\n"
        f"  Task   : {task_id}\n"
        f"  Run ID : {run_id}\n"
        f"  Feil   : {exc}\n"
        f"  Logg   : {log_url}"
    )

    # Alltid logg til Airflow-loggen
    context["task_instance"].log.error(message)

    # Valgfri Slack-varsling — krev at variabelen er satt i Airflow UI
    slack_url = Variable.get("slack_webhook_url", default_var=None)
    if slack_url:
        try:
            from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
            SlackWebhookHook(webhook_token=slack_url).send(text=message)
        except Exception as e:
            context["task_instance"].log.warning(f"Slack-varsling feilet: {e}")


def log_pipeline_success(**context):
    """Avsluttende task som bekrefter at hele pipeline er fullført."""
    run_id   = context["run_id"]
    exec_date = context["ds"]
    context["task_instance"].log.info(
        f"✅ Slettix Analytics pipeline fullført\n"
        f"  Exec date : {exec_date}\n"
        f"  Run ID    : {run_id}\n"
        f"  Kjørerekke: ingest → bronze → silver → gold ✓"
    )


# ── DAG-definisjon ─────────────────────────────────────────────────────────

default_args = {
    "owner":              "slettix",
    "retries":            2,
    "retry_delay":        timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "email_on_failure":   False,   # sett True + konfigurer SMTP for e-postvarsling
    "email_on_retry":     False,
    "on_failure_callback": on_task_failure,
}

with DAG(
    dag_id="01_slettix_pipeline",
    description="End-to-end pipeline: Ingest → Bronze → Silver → Gold",
    schedule="@daily",
    start_date=datetime(2024, 1, 15),
    catchup=False,
    default_args=default_args,
    tags=["slettix", "medallion"],
    doc_md=__doc__,
) as dag:

    ingest = SparkSubmitOperator(
        task_id="ingest_employees_to_bronze",
        application="/opt/airflow/spark_jobs/ingest_to_bronze.py",
        conn_id=SPARK_CONN_ID,
        packages=DELTA_JARS,
        conf=SPARK_CONF,
        application_args=[
            "--source", "s3a://raw/employees",
            "--target", "s3a://bronze/employees",
            "--format", "csv",
            "--ingestion-date", "{{ ds }}",
        ],
        name="ingest_employees_to_bronze",
        execution_timeout=timedelta(minutes=15),
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver_employees",
        application="/opt/airflow/spark_jobs/transform_bronze_to_silver.py",
        conn_id=SPARK_CONN_ID,
        packages=DELTA_JARS,
        conf=SPARK_CONF,
        application_args=[
            "--config", "/opt/airflow/spark_conf/silver/employees.json",
        ],
        name="bronze_to_silver_employees",
        execution_timeout=timedelta(minutes=15),
    )

    build_gold = SparkSubmitOperator(
        task_id="build_gold_department_stats",
        application="/opt/airflow/spark_jobs/build_gold_table.py",
        conn_id=SPARK_CONN_ID,
        packages=DELTA_JARS,
        conf=SPARK_CONF,
        application_args=[
            "--config", "/opt/airflow/spark_conf/gold/department_stats.json",
        ],
        name="build_gold_department_stats",
        execution_timeout=timedelta(minutes=15),
    )

    complete = PythonOperator(
        task_id="pipeline_complete",
        python_callable=log_pipeline_success,
    )

    ingest >> bronze_to_silver >> build_gold >> complete
