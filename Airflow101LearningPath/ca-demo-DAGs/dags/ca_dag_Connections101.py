# Import Statements
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import chain
from airflow.decorators import dag, task
import pendulum
from datetime import timedelta



# Define your UDFs
def print_1():
    print('Hello from taks_1')



# Define your DAG Object
with DAG('ca_dag_Connections101',
         description = 'First DAG',
         start_date = datetime(2024,12,10),
         schedule = timedelta(days=2),
         tags = ['DEs'],
         catchup = True 
    ):
    
    # Create your tasks
    task_1 = PythonOperator(task_id = 'task_1',
                            python_callable = print_1
                        )
    
    # Define Orchestration Logic (task 1 then (task 2 and task 3 in parallel), then task 4)
    task_1