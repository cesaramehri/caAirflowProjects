from airflow.decorators import dag, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=['aws'],
    catchup=False
)
def my_dag():

    wait_for_file = S3KeySensor(
        task_id="wait_for_file",
        aws_conn_id="aws_s3",
        bucket_key="s3://marc-airflow/data_*",
        wildcard_match=True,
    )

    @task
    def process_file():
        print("I processed the file!")

    wait_for_file >> process_file()

my_dag()