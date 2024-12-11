from airflow import DAG
from airflow.decorators import task
from datetime import datetime

# Define DAG Object
with DAG(dag_id = 'ca_dag_Connections101',
         start_date = datetime(2024,12,10),
         schedule_interval = '@daily',
         catchup = False
):
    
    # # Create Tasks (ti = task instance object)
    # @task
    # def sender_task(ti=None):
    #     return 'iPhone'
    
    # @task
    # def receiver_task(mobile_phone):
    #     print(mobile_phone)

    # # Orchestract your Tasks
    # receiver_task(sender_task())

    # # Create Tasks, pushing/pulling value with key
    # @task
    # def sender_task(ti=None):
    #     ti.xcom_push(key = 'mobile_phone', value = 'iPhone')
    
    # @task
    # def receiver_task(ti=None):
    #     msg = ti.xcom_pull(task_ids = 'sender_task', key = 'mobile_phone')
    #     print(msg)

    # # Orchestract your Tasks
    # sender_task() >> receiver_task()

    # Create Tasks, pushing/pulling value with key
    @task
    def sender1_task(ti=None):
        ti.xcom_push(key = 'mobile_phone', value = 'iPhone')
    
    @task
    def sender2_task(ti=None):
        ti.xcom_push(key = 'mobile_phone', value = 'Galaxy')
    
    @task
    def receiver_task(ti=None):
        msg = ti.xcom_pull(task_ids = ['sender1_task', 'sender2_task'], key = 'mobile_phone')
        print(msg)

    # Orchestract your Tasks
    [sender1_task() >> sender2_task()] >> receiver_task()