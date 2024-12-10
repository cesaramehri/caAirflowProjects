from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Define your DAG Object
with DAG(dag_id='ca_dag002', 
         schedule='@daily', 
         start_date=datetime(2024, 12, 10), 
         description='DAG to check data deposit', 
         tags = ['DEs'],
         catchup=False
    ):
    
    # Create your tasks
    create_file_task = BashOperator(task_id='create_file_task',
                                    bash_command='echo "This is my second dag!" >./tmp/dummy/file.txt'
                                )

    check_file_exists_task = BashOperator(task_id='check_file_exists_task',
                                          bash_command='test -f ./tmp/dummy/file.txt'
                                        )

    read_file_task = PythonOperator(task_id='read_file_task',
                                    python_callable=lambda: print(open('./tmp/dummy/file.txt', 'rb').read())
                                )

    # Orchestrate your Tasks
    create_file_task >> check_file_exists_task >> read_file_task