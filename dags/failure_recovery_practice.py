from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def intentional_failure():
    print("Data validation completed successfully")


with DAG(
    dag_id="failure_recovery_practice",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["practice", "troubleshooting"],
) as dag:
    fail_task = PythonOperator(
        task_id="validate_data",
        python_callable=intentional_failure,
        retries=0,
    )
