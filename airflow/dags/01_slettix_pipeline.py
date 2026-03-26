"""
Slettix Analytics — end-to-end pipeline DAG

Kjørerekke:
  ingest_employees_to_bronze
      → bronze_to_silver_employees
          → build_gold_department_stats

Kan kjøres manuelt fra Airflow UI eller trigges daglig (se schedule).
SparkSubmitOperator sender jobber til spark://spark-master:7077.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_ID = "spark_default"

# Spark configuration passed to every task
# (mirrors spark-defaults.conf, needed because the driver runs in Airflow container)
SPARK_CONF = {
    "spark.sql.extensions":                     "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog":          "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.endpoint":             "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key":           "admin",
    "spark.hadoop.fs.s3a.secret.key":           "changeme",
    "spark.hadoop.fs.s3a.path.style.access":    "true",
    "spark.hadoop.fs.s3a.impl":                 "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.sql.shuffle.partitions":             "8",
    # Driver runs in Airflow container — workers need to reach back to it
    "spark.driver.host":                        "airflow-scheduler",
    "spark.driver.bindAddress":                 "0.0.0.0",
}

# JARs needed by driver (same as those baked into the Spark image)
DELTA_JARS = ",".join([
    "io.delta:delta-spark_2.12:3.2.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])

default_args = {
    "owner":            "slettix",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="01_slettix_pipeline",
    description="Ingest → Bronze → Silver → Gold for employees dataset",
    schedule="@daily",
    start_date=datetime(2024, 1, 15),
    catchup=False,
    default_args=default_args,
    tags=["slettix", "medallion"],
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
            "--ingestion-date", "{{ ds }}",   # Airflow execution date
        ],
        name="ingest_employees_to_bronze",
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
    )

    ingest >> bronze_to_silver >> build_gold
