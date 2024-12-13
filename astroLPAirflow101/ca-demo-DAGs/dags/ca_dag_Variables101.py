from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from datetime import datetime

# Define your UDFs
def _ml_task(ml_params):
    print(ml_params)

# Define DAG Object
with DAG(dag_id = 'ca_dag_Variables101',
         start_date = datetime(2024,12,10),
         schedule_interval = '@daily',
         catchup = False
) as dag:
    # Create Tasks (var is defined in the UI)
    for ml_params in Variable.get('ml_model_params', deserialize_json = True)["param"]:
        PythonOperator(task_id = f'ml_task_{ml_params}',
                       python_callable = _ml_task,
                       op_kwargs = {'ml_params': ml_params}
                    )
    #