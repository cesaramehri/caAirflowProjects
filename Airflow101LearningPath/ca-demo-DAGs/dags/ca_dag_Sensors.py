from airflow import DAG
from airflow.sensors.python import PythonSensor

from datetime import datetime

def _condition():
    return False

with DAG(
    dag_id="sensor",
    start_date=datetime(2024, 12, 10),
    schedule="@daily",
    catchup=False,
):
    waiting_for_condition = PythonSensor(
        task_id="waiting_for_condition",
        python_callable=_condition,
        poke_interval=60,
        timeout=7 * 24 * 60 * 60
    )
