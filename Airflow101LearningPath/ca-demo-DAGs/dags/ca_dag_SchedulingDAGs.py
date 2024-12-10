from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
import pendulum
from datetime import timedelta

# Define your UDFs
def print_1():
    print('Hello from taks_1')

# Create DAG object
with DAG(dag_id = 'ca_dag_SchedulingDAGs',
         schedule = timedelta(hours=2),
         start_date = pendulum.datetime(2024,12,10),
         catchup = True
):
    
    # Create Tasks
    task_1 = PythonOperator(task_id = 'task_1',
                            python_callable = print_1
                        )

    # Orchestrate your Tasks
    task_1()