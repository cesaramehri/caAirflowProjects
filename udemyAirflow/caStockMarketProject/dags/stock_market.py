from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from datetime import datetime
import requests

from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv



SYMBOL = 'AAPL'
BUCKET_NAME = 'stock-market'


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
        # First, Go and Create a connection in the Airflow UI (check AirflowUI/25. API_Connection.png) 
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

    # Task 3: Ingest the data in minio (http://localhost:9001/login)
    store_stock_prices = PythonOperator(task_id = 'store_stock_prices',
                                        python_callable = _store_prices,
                                        op_kwargs = {'stock_prices_str': '{{task_instance.xcom_pull(task_ids="get_stock_prices")}}'}                         
                                    )

    # Task 4: Process Data using Spark (See folder: spark)
        # First, build your spark image: cd .\spark\notebooks\stock_transform\ => docker build . -t airflow/spark-app
        # Second, install docker operator: go to requirements.txt, and under minio, add docker operator
    format_prices = DockerOperator(task_id = 'format_prices',
                                   image = 'airflow/spark-app',
                                   container_name = 'format_prices',
                                   api_version = 'auto',
                                   auto_remove = True,
                                   docker_url = 'tcp://docker-proxy:2375',
                                   network_mode = 'container:spark-master',
                                   tty = True,
                                   xcom_all = False,
                                   mount_tmp_dir = False,
                                   environment = {'SPARK_APPLICATION_ARGS': '{{task_instance.xcom_pull(task_ids="store_stock_prices")}}'}
                                )
    
    # Task 5: Extract formated prices from minio
    get_formatted_csv = PythonOperator(task_id = 'get_formatted_csv',
                                       python_callable = _get_formatted_csv,
                                       op_kwargs = {'path_location': '{{task_instance.xcom_pull(task_ids="store_stock_prices")}}'}                         
                                    )

    # Task 6: Deliver into the DW
        # First, create a connection for postgres (check postgres/30.postgres_conn)
        # Second, USe astro-sdk -> load_file_operator (pre-installed with the astro cli)
    load_to_dw = aql.load_file(task_id = 'load_to_dw',
                               input_file=File(path=f"s3://{BUCKET_NAME}/{{{{ task_instance.xcom_pull(task_ids='get_formatted_csv')}}}}", conn_id='minio_conn'),
                               output_table = Table(name=SYMBOL, conn_id='postgres', metadata=Metadata(schema='public'))
                            )
    # load_to_dw = aql.load_file(task_id = 'load_to_dw',
    #                            input_file=File(path='{{ ti.xcom_pull(task_ids="get_formatted_csv") }}', conn_id='minio_conn'),
    #                            output_table = Table(name=SYMBOL, conn_id='postgres', metadata=Metadata(schema='public'))
    #                         )

    # Define your Dependencies
    is_api_available() >> get_stock_prices >> store_stock_prices >> format_prices >> get_formatted_csv >> load_to_dw



# Call your dag function
stock_market()