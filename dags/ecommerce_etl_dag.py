from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

def generate_data():
    subprocess.run(["python", "generate_data.py"])

def load_data():
    subprocess.run(["python", "load_data.py"])

default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2025, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="ecommerce_etl_dag",
    default_args=default_args,
    schedule_interval="@daily",  # runs every day
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="generate_mock_data",
        python_callable=generate_data
    )

    task2 = PythonOperator(
        task_id="load_data_to_warehouse",
        python_callable=load_data
    )

    task1 >> task2
