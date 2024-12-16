from airflow.hooks.base import BaseHook
from minio import Minio                                 # add minio to requirements.txt
from airflow.exceptions import AirflowNotFoundException
import requests
import json
from io import BytesIO



# UDFs
# Get data from the api
def _get_stock_prices(url, symbol):
    api = BaseHook.get_connection('stock_api')
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    response = requests.get(url, headers=api.extra_dejson['headers'])
    stock_prices = json.dumps(response.json()['chart']['result'][0])
    return stock_prices



# Store data in a bucket
def _store_prices(stock_prices_str):

    # First, Go and Create a connection (check AirflowUI/25. minio_Connection.png) in the Airflow UI
    minio = BaseHook.get_connection('minio_conn')
    client = Minio(endpoint = minio.extra_dejson['endpoint_url'].split('//')[1],
                   access_key = minio.login,
                   secret_key = minio.password,
                   secure = False
            )

    # Get symbol and data
    stock_prices_dict = json.loads(stock_prices_str)
    stock_symbol = stock_prices_dict['meta']['symbol']
    stock_data_str = json.dumps(stock_prices_dict, ensure_ascii=False).encode('utf8')
    
    # Create a bucket
    bucket_name = 'stock-market'
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    
    # Store into bucket
    objw = client.put_object(bucket_name = bucket_name,
                             object_name = f'{stock_symbol}/prices.json',
                             data = BytesIO(stock_data_str),
                             length = len(stock_data_str)
                        )
    
    return f'{objw.bucket_name}/{stock_symbol}'



# Extract data from a bucket
def _get_formatted_csv(path):
    #path = 'stock-market/AAPL'
    # Connect to minio client
    minio = BaseHook.get_connection('minio_conn')
    client = Minio(endpoint = minio.extra_dejson['endpoint_url'].split('//')[1],
                   access_key = minio.login,
                   secret_key = minio.password,
                   secure = False
            )
    
    # Define object params
    bucket_name = 'stock-market'
    prefix_name = f"{path.split('/')[1]}/formatted_prices/"
    objects = client.list_objects(bucket_name, prefix=prefix_name, recursive=True)

    # Loop through
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name
    raise AirflowNotFoundException('csv file not found')
