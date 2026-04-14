from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Dynamic project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(PROJECT_ROOT, "scripts"))

from pipeline import run_pipeline

default_args = {
    "owner": "data-engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="walmart_retail_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["retail", "etl"]
) as dag:

    run_etl = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline
    )

    run_etl
