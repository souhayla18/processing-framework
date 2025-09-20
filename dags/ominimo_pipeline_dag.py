from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

"""Waits until the input file exists (FileSensor).

Then runs your pipeline (main.py).

Can be extended later with downstream tasks."""


default_args = {
    "owner": "souhayla",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="ominimo_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ominimo","pipelines"]
) as dag:

    wait_for_input = FileSensor(
        task_id="wait_for_input",
        filepath="/opt/airflow/data/input/motor_policy.json",
        poke_interval=30,
        timeout=600,
        mode="poke"
    )

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command="python /opt/airflow/src/main.py",
    )

    wait_for_input >> run_pipeline
