"""
Folkeregister IDP — overvåkings-DAG

Denne DAG-en overvåker to Kafka-til-Bronze streaming-jobber som konsumerer
hendelser fra det nasjonale Folkeregisteret:

  folkeregister_person_idp  →  bronze/folkeregister/person_events
  folkeregister_family_idp  →  bronze/folkeregister/family_events

Kjørerekke (hvert 15. minutt):
  check_person_freshness ──┐
                           ├──► check_consumer_lag ──► register_dag_link
  check_family_freshness ──┘

Oppgavebeskrivelser:
  check_person_freshness  — sjekker at Bronze mottar ferske data fra person-topics
  check_family_freshness  — sjekker at Bronze mottar ferske data fra familie-topics
  check_consumer_lag      — beregner Kafka consumer group lag og varsler ved høy lag
  register_dag_link       — patcher dag_id inn i begge produktmanifester i portalen

Slik starter du streaming-jobbene (Spark Operator på Kubernetes):

  kubectl apply -f k8s/spark/folkeregister_person_idp.yaml
  kubectl apply -f k8s/spark/folkeregister_family_idp.yaml

  # Sjekk status:
  kubectl get sparkapplication -n slettix-analytics

Kafka UI: http://localhost:8084/ui/clusters/customermaster
"""

import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

PORTAL_URL = os.environ.get("PORTAL_URL", "http://dataportal.slettix-analytics.svc.cluster.local:8090")
PORTAL_API_KEY = os.environ.get("PORTAL_API_KEY", "dev-key-change-me")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-bootstrap.customermaster.svc.cluster.local:9092",
)

MINIO_STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL":           os.environ.get("MINIO_ENDPOINT", "http://minio.slettix-analytics.svc.cluster.local:9000"),
    "AWS_ACCESS_KEY_ID":          os.environ.get("MINIO_ACCESS_KEY", "admin"),
    "AWS_SECRET_ACCESS_KEY":      os.environ.get("MINIO_SECRET_KEY", "changeme"),
    "AWS_ALLOW_HTTP":             "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

# Consumer groups definert i Spark-jobbene
PERSON_CONSUMER_GROUP = "slettix-folkeregister-person-idp"
FAMILY_CONSUMER_GROUP = "slettix-folkeregister-family-idp"
PERSON_TOPICS = ["person.registered", "person.born", "person.died", "person.moved"]
FAMILY_TOPICS = ["family.married", "family.divorced", "family.child_born"]

# Maksimal alder på siste prosesserte hendelse før vi varsler (minutter)
FRESHNESS_WARN_MINUTES = 5

# Maksimal consumer group lag før vi varsler
LAG_WARN_THRESHOLD = 10_000


# ── Feilhåndtering ─────────────────────────────────────────────────────────

def on_task_failure(context):
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id  = context["run_id"]
    exc     = context.get("exception")
    log_url = context["task_instance"].log_url

    message = (
        f"Folkeregister IDP — task feilet\n"
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


# ── Task-funksjoner ────────────────────────────────────────────────────────

def _check_bronze_freshness(bronze_path: str, product_id: str, **context):
    """
    Les siste _processed_at fra Bronze Delta-tabell og varsle hvis dataene er for gamle.
    Krever at delta-rs er installert (deltalake-pakken) — ingen Spark nødvendig.
    """
    import json
    from datetime import datetime, timezone

    import boto3
    from botocore.client import Config

    log = context["task_instance"].log

    try:
        from deltalake import DeltaTable

        dt = DeltaTable(bronze_path.replace("s3a://", "s3://"), storage_options=MINIO_STORAGE_OPTIONS)
        df = dt.to_pandas(columns=["_processed_at"])

        if df.empty:
            log.warning(f"[freshness] {product_id}: Bronze-tabell er tom — ingen data mottatt ennå")
            return

        latest = df["_processed_at"].max()
        # Konverter til UTC hvis nødvendig
        if hasattr(latest, "tzinfo") and latest.tzinfo is None:
            import pandas as pd
            latest = pd.Timestamp(latest).tz_localize("UTC")

        age_minutes = (datetime.now(tz=timezone.utc) - latest).total_seconds() / 60
        log.info(f"[freshness] {product_id}: Siste _processed_at = {latest} ({age_minutes:.1f} min siden)")

        if age_minutes > FRESHNESS_WARN_MINUTES:
            log.warning(
                f"[freshness] ADVARSEL: {product_id} — siste data er {age_minutes:.1f} min gammel "
                f"(terskel: {FRESHNESS_WARN_MINUTES} min). Sjekk at streaming-jobben kjører."
            )
        else:
            log.info(f"[freshness] {product_id}: OK — data er ferske ({age_minutes:.1f} min)")

    except Exception as exc:
        log.warning(
            f"[freshness] {product_id}: Kunne ikke lese Bronze-tabell ({exc}). "
            f"Sannsynlig årsak: streaming-jobben har ikke startet ennå."
        )


def check_person_freshness(**context):
    _check_bronze_freshness(
        bronze_path="s3a://bronze/folkeregister/person_events",
        product_id="folkeregister.person_events",
        **context,
    )


def check_family_freshness(**context):
    _check_bronze_freshness(
        bronze_path="s3a://bronze/folkeregister/family_events",
        product_id="folkeregister.family_events",
        **context,
    )


def check_consumer_lag(**context):
    """
    Kobler til Kafka og beregner consumer group lag for begge IDP consumer groups.
    Varsler i Airflow-loggen hvis lag overskrider LAG_WARN_THRESHOLD.
    """
    log = context["task_instance"].log

    try:
        from kafka import KafkaConsumer, TopicPartition
        from kafka.admin import KafkaAdminClient
    except ImportError:
        log.warning(
            "[lag] kafka-python ikke installert — kan ikke sjekke consumer lag. "
            "Installer med: pip install kafka-python"
        )
        return

    groups = [
        (PERSON_CONSUMER_GROUP, PERSON_TOPICS),
        (FAMILY_CONSUMER_GROUP, FAMILY_TOPICS),
    ]

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id="airflow-lag-monitor",
            request_timeout_ms=10_000,
        )
    except Exception as exc:
        log.warning(
            f"[lag] Kunne ikke koble til Kafka på {KAFKA_BOOTSTRAP_SERVERS}: {exc}. "
            f"Sjekk at Kafka kjører og at KAFKA_BOOTSTRAP_SERVERS er riktig satt."
        )
        return

    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    total_lag_all_groups = 0

    for group_id, topics in groups:
        group_lag = 0

        try:
            committed_offsets = admin.list_consumer_group_offsets(group_id)
        except Exception as exc:
            log.warning(f"[lag] Kunne ikke hente committed offsets for {group_id}: {exc}")
            continue

        for topic in topics:
            try:
                partitions_for_topic = consumer.partitions_for_topic(topic)
                if not partitions_for_topic:
                    log.warning(f"[lag] Topic '{topic}' ikke funnet i Kafka-clusteret")
                    continue

                tps = [TopicPartition(topic, p) for p in partitions_for_topic]
                end_offsets = consumer.end_offsets(tps)

                for tp in tps:
                    end = end_offsets.get(tp, 0)
                    meta = committed_offsets.get(tp)
                    committed = meta.offset if meta else 0
                    partition_lag = max(0, end - committed)
                    group_lag += partition_lag

            except Exception as exc:
                log.warning(f"[lag] Feil ved lag-beregning for topic '{topic}': {exc}")

        total_lag_all_groups += group_lag
        log_fn = log.warning if group_lag > LAG_WARN_THRESHOLD else log.info
        log_fn(f"[lag] {group_id}: total lag = {group_lag:,} meldinger")

        if group_lag > LAG_WARN_THRESHOLD:
            log.warning(
                f"[lag] ADVARSEL: Consumer group '{group_id}' har {group_lag:,} meldinger i kø "
                f"(terskel: {LAG_WARN_THRESHOLD:,}). Sjekk at streaming-jobben kjører."
            )

    log.info(f"[lag] Samlet lag på tvers av alle grupper: {total_lag_all_groups:,} meldinger")

    try:
        consumer.close()
        admin.close()
    except Exception:
        pass


def register_dag_link(**context):
    """
    Patcher dag_id inn i produktmanifestene for begge IDP-produkter i portalen.
    Idempotent — trygt å kjøre gjentatte ganger.
    """
    import requests

    dag_id  = context["dag"].dag_id
    log     = context["task_instance"].log
    headers = {"X-API-Key": PORTAL_API_KEY}

    products = ["folkeregister.person_events", "folkeregister.family_events"]
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
    "retries":                   1,
    "retry_delay":               timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "email_on_failure":          False,
    "email_on_retry":            False,
    "on_failure_callback":       on_task_failure,
}

with DAG(
    dag_id="folkeregister_idp",
    description="Folkeregister IDP — overvåker Kafka-til-Bronze streaming-jobber",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 15),
    catchup=False,
    default_args=default_args,
    tags=["folkeregister", "idp", "streaming", "kafka"],
    doc_md=__doc__,
) as dag:

    check_person = PythonOperator(
        task_id="check_person_freshness",
        python_callable=check_person_freshness,
        execution_timeout=timedelta(minutes=5),
    )

    check_family = PythonOperator(
        task_id="check_family_freshness",
        python_callable=check_family_freshness,
        execution_timeout=timedelta(minutes=5),
    )

    check_lag = PythonOperator(
        task_id="check_consumer_lag",
        python_callable=check_consumer_lag,
        execution_timeout=timedelta(minutes=5),
    )

    register_link = PythonOperator(
        task_id="register_dag_link",
        python_callable=register_dag_link,
        execution_timeout=timedelta(minutes=2),
    )

    [check_person, check_family] >> check_lag >> register_link
