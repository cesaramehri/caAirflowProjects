from airflow.hooks.base import BaseHook
import requests
import json


def _get_stock_prices(url, symbol):
    api = BaseHook.get_connection('stock_api')
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])