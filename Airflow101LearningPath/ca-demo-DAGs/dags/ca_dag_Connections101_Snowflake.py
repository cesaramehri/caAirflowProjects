from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# Define your DAG object
with DAG(dag_id = 'ca_dag_Connections101_Snowflake',
         description = 'First Connection to Snowflake from Airflow',
         start_date = datetime(2024,12,10),
         schedule_interval = '@daily',
         tags = ['DEs'],
         catchup = False 
    ):
    
    # Create your tasks
    execute_request_task = SnowflakeOperator(task_id = 'execute_request_task',
                                             conn_id = 'snowflake_conn',
                                             sql = 'SELECT * FROM GARDEN_PLANTS.VEGGIES.ROOT_DEPTH'

    )

    # Orchestrate Tasks
    execute_request_task
