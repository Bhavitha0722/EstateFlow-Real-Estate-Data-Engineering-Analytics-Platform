import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from analytics_db import load_curated_data
from clean import pre_process
from data_quality import validate_data_quality
from filter import filter_function
from notifications import notify_pipeline_failure, notify_stale_data

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 8, 22),
    "on_failure_callback": notify_pipeline_failure,
}


with DAG(
    dag_id="real_estate_analytics", default_args=default_args, schedule="@daily", catchup=False
) as dag:

    def check_file_exists():
        input_path = Path("/opt/airflow/input_files/modified_data.csv")
        if not input_path.exists():
            raise FileNotFoundError(input_path)
        print(f"Input file found: {input_path.stat().st_size} bytes")

    check_file = PythonOperator(
        task_id="check_file",
        python_callable=check_file_exists,
        retries=2,
        retry_delay=timedelta(seconds=15),
    )

    data_quality_task = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_data_quality,
    )

    pre_process_task = PythonOperator(
        task_id="pre_process",
        python_callable=pre_process,
        op_kwargs={"process_date": "{{ data_interval_start | ds }}"},
    )

    filter_task = PythonOperator(task_id="filter_data", python_callable=filter_function)

    def check_data_freshness():
        threshold_hours = int(os.getenv("FRESHNESS_THRESHOLD_HOURS", "24"))
        hook = PostgresHook(postgres_conn_id="analytics_db")
        with hook.get_conn() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT MAX(loaded_at)::text AS data_loaded_at FROM real_estate_listings"
                )
                row = cursor.fetchone()
        notify_stale_data(row[0] if row else None, threshold_hours=threshold_hours)

    load_database_task = PythonOperator(
        task_id="load_curated_data",
        python_callable=load_curated_data,
        op_kwargs={"postgres_conn_id": "analytics_db"},
        retries=2,
        retry_delay=timedelta(minutes=1),
    )

    freshness_check_task = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
    )

    (
        check_file
        >> data_quality_task
        >> pre_process_task
        >> filter_task
        >> load_database_task
        >> freshness_check_task
    )
