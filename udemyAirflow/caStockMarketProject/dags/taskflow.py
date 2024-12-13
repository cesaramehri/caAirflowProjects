from airflow.decorators import dag, task
from datetime import datetime



# Define your dag object
@dag(
    start_date = datetime(2024,1,1),
    schedule = '@daily',
    catchup = False,
    tags = ['taskflow']
)

# Define dag function, which name is the unique id of your dag
def taskflow():
    
    # Define your Tasks
    @task
    def task_a():
        print("Task A")
        return 42

    @task
    def task_b(value):
        print("Task B")
        print(value)

    # Define your Dependencies
    task_b(task_a())

# Call your dag function
taskflow()