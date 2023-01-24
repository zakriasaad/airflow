from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
import requests
import time
import json
import os
from datetime import datetime,timedelta


def get_data(**kwargs):
    ticker=kwargs['ticker']
    key="ZQZCXMVHMKGZUNKC"
    url ='https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=' + ticker + '&interval=5min&apikey='+key
    r = requests.get(url)
    
    try:
        data=r.json()
        path=r"C:\Users\zakria\Desktop\raw_data"
        with open(path+"stock_market_raw_data_" + ticker + '_' +str(time.time()),"w") as outfile:
            json.dump(data,outfile)
    except:
        pass


default_args={
    'start_date':datetime(2023,2,1),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'project_id':1
}

with DAG("market_data_alphavantage_dag", schedule_interval = '@daily', catchup=False, default_args = default_args) as dag_python:

    task_0 = PythonOperator(task_id = "get_market_data", python_callable = get_data, op_kwargs = {'tickers' : []})
