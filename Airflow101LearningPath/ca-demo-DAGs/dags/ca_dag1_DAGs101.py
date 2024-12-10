# Import Statements
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import chain

# Define your UDFs
def print_1():
    print('Hello from taks_1')
def print_2():
    print('Hello from taks_2')
def print_3():
    print('Hello from taks_3')
def print_4():
    print('Hello from taks_4')
def print_5():
    print('Hello from taks_5')

# Define your DAG Object
with DAG('ca_dag1_DAGs101',
         start_date = datetime(2024,12,10),
         description = 'First DAG',
         tags = ['DEs'],
         schedule = '@daily',
         catchup = False 
    ):
    
    # Create your tasks
    task_1 = PythonOperator(task_id = 'task_1',
                            python_callable = print_1
                        )
    task_2 = PythonOperator(task_id = 'task_2',
                            python_callable = print_2
                        )
    task_3 = PythonOperator(task_id = 'task_3',
                            python_callable = print_3
                        )
    task_4 = PythonOperator(task_id = 'task_4',
                            python_callable = print_4
                        )
    task_5 = PythonOperator(task_id = 'task_5',
                            python_callable = print_5
                        )
    
    # Define Orchestration Logic (task 1 then (task 2 and task 3 in parallel), then task 4)
    #task_1 >> [task_2, task_3, task_4] >> task_5

    chain(task_1, [task_2, task_3], [task_4, task_5])