from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

from include.stock_market.tasks import _get_stock_prices, _store_prices



SYMBOL = 'AAPL'



# Define your dag object
@dag(
    start_date = datetime(2024,1,1),
    schedule = '@daily',
    catchup = False,
    tags = ['stock_market']
)



# Define dag function, which name is the unique id of your dag
def stock_market():
    
    # Define your Tasks
    # Task 1: Check API if available
    @task.sensor(poke_interval = 30,                    # Check the container every 30 seconds
                 timeout = 300,                         # Timeout the sensor after 5minutes
                 mode = 'poke')
    def is_api_available() -> PokeReturnValue:
        # First, Go and Create a connection (check AirflowUI/25. API_Connection.png) in the Airflow UI
        api = BaseHook.get_connection('stock_api')                           # Fetch connection
        url = f"{api.host}{api.extra_dejson['endpoint']}"                    # Construct url from api's host and endpoints
        response = requests.get(url, headers=api.extra_dejson['headers'])    # Make request to the url 
        condition = response.json()['finance']['result'] is None             # Define the condition (if the api is available) of the sensor
        return PokeReturnValue(is_done=condition, xcom_value=url)

    # Task 2: Fetch data (check in the ui, admin -> xcoms)
    get_stock_prices = PythonOperator(task_id = 'get_stock_prices',
                                      python_callable = _get_stock_prices,
                                      op_kwargs = {'url': '{{task_instance.xcom_pull(task_ids="is_api_available")}}',
                                                   'symbol': SYMBOL
                                                }
                                )

    # Task 3: Save the data in minio (http://localhost:9001/login)
    store_stock_prices = PythonOperator(task_id = 'store_stock_prices',
                                        python_callable = _store_prices,
                                        op_kwargs = {'stock_prices_str': '{{task_instance.xcom_pull(task_ids="get_stock_prices")}}'}                         
                                    )



    # Define your Dependencies
    is_api_available() >> get_stock_prices >> store_stock_prices



# Call your dag function
stock_market()