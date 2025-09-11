from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import subprocess

# ------------------------------
# DAG definition
# ------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 11),
    "retries": 1,
}

dag = DAG(
    "ecommerce_etl_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

# ------------------------------
# Step 1: Generate Bronze data
# ------------------------------
generate_data = BashOperator(
    task_id="generate_bronze_data",
    bash_command="python /path/to/your/project/generate_data.py",
    dag=dag,
)

# ------------------------------
# Step 2: Clean Silver data
# ------------------------------
clean_data = BashOperator(
    task_id="clean_silver_data",
    bash_command="python /path/to/your/project/transform/clean_data.py",
    dag=dag,
)

# ------------------------------
# Step 3: Aggregate Gold data
# ------------------------------
aggregate_data = BashOperator(
    task_id="aggregate_gold_data",
    bash_command="python /path/to/your/project/transform/aggregate_data.py",
    dag=dag,
)

# ------------------------------
# Step 4: Incremental Load
# ------------------------------
incremental_load = BashOperator(
    task_id="incremental_load_to_warehouse",
    bash_command="python /path/to/your/project/incremental/incremental_load.py",
    dag=dag,
)

# ------------------------------
# Step 5: Data Validation
# ------------------------------
def run_data_validation():
    subprocess.run(["python", "/path/to/your/project/validation/validate_data.py"], check=True)

data_validation = PythonOperator(
    task_id="validate_data",
    python_callable=run_data_validation,
    dag=dag,
)

# ------------------------------
# Step 6: ML LTV Training
# ------------------------------
def train_ltv_model():
    subprocess.run(["python", "/path/to/your/project/ml/train_ltv_model.py"], check=True)

train_ltv = PythonOperator(
    task_id="train_ltv_model",
    python_callable=train_ltv_model,
    dag=dag,
)

# ------------------------------
# DAG Task Dependencies
# ------------------------------
generate_data >> clean_data >> aggregate_data >> incremental_load
incremental_load >> data_validation >> train_ltv
