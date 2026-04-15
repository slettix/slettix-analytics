"""
Airflow DAG — Analytisk dataprodukt: Employee Satisfaction Score
Produkt-ID : analytics.employees_satisfaction
Auto-generert av Slettix Analytics Portal
"""
from datetime import datetime
from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator

with DAG(
    dag_id="analytical_employees",
    schedule_interval='@monthly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["analytical", "analytics"],
    doc_md="""
    Kjører analytisk notebook for **Employee Satisfaction Score** og publiserer resultatet
    som dataprodukt `analytics.employees_satisfaction` i Slettix Data Portal.
    """,
) as dag:
    run_notebook = PapermillOperator(
        task_id="run_notebook",
        input_nb="/home/spark/notebooks/product_analytics_employees_satisfaction.ipynb",
        output_nb="/home/spark/notebooks/output/analytics_employees_satisfaction_{{ ds }}.ipynb",
        parameters={"run_date": "{{ ds }}"},
    )
