from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess


# --- Python callables for Airflow tasks ---
def generate_data():
    subprocess.run(["python", "generate_data.py"], check=True)


def clean_data():
    subprocess.run(["python", "transform/clean_data.py"], check=True)


def aggregate_data():
    subprocess.run(["python", "transform/aggregate_data.py"], check=True)


def incremental_load():
    subprocess.run(["python", "incremental/incremental_load.py"], check=True)


# --- Airflow DAG configuration ---
default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="ecommerce_etl_dag",
    default_args=default_args,
    description="ETL pipeline for E-commerce Data Warehouse",
    schedule_interval="@daily",  # Run once per day
    catchup=False,
    tags=["ecommerce", "etl", "warehouse"],
) as dag:

    task1 = PythonOperator(
        task_id="generate_mock_data",
        python_callable=generate_data
    )

    task2 = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data
    )

    task3 = PythonOperator(
        task_id="aggregate_data",
        python_callable=aggregate_data
    )

    task4 = PythonOperator(
        task_id="incremental_load_to_warehouse",
        python_callable=incremental_load
    )

    # DAG flow: Bronze → Silver → Gold → Warehouse
    task1 >> task2 >> task3 >> task4
