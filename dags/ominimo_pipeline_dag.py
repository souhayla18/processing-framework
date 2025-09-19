from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

def run_pipeline():
    subprocess.run(["python", "src/main.py"], check=True)

with DAG(
    dag_id="ominimo_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    run = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline
    )
