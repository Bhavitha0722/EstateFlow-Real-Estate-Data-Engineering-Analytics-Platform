from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from analytics_db import load_curated_data
from clean import pre_process, validate_input_file
from filter import filter_function


def check_supply_data() -> None:
    validate_input_file()


default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 8, 25),
}


with DAG(
    dag_id="supply_chain_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:
    check_data = PythonOperator(
        task_id="check_data",
        python_callable=check_supply_data,
    )

    process_data = PythonOperator(
        task_id="process_data",
        python_callable=pre_process,
        op_kwargs={"process_date": "{{ ds }}"},
    )

    filter_data = PythonOperator(
        task_id="filter_data",
        python_callable=filter_function,
    )

    load_data = PythonOperator(
        task_id="load_curated_data",
        python_callable=load_curated_data,
        op_kwargs={"postgres_conn_id": "analytics_db"},
    )

    check_data >> process_data >> filter_data >> load_data
