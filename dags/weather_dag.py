import sys
import os
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
import time
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


file_path = os.path.abspath(__file__) #gets the absolute path of the current file
DAG_FOLDER = os.path.dirname(file_path) #gets the directory name of the file path
SCRIPT_FOLDER = os.path.join(DAG_FOLDER, 'weather_etl') #constructs the path to the 'weather_etl' folder
sys.path.append(SCRIPT_FOLDER)  
import main

default_args = {
'owner' : 'pker',

}

with DAG(
    dag_id = 'weather_dag',
    description = 'Extract weather data from api',
    default_args = default_args,
    start_date = days_ago(5),
    schedule_interval = '@daily',
    tags = ['weather', 'data'],
    catchup = True
) as dag:
    extract_data = PythonOperator(
        task_id = 'extract_weather_data',
        python_callable = main.fetch_weather_data,
        op_kwargs = {'name': 'pker'}
    )
    transform_data = PythonOperator(
        task_id = 'transform_weather_data',
        python_callable = main.transform_weather_data,
        op_kwargs = {'data' : extract_data.output}

    )
    load_data = PythonOperator(
        task_id = 'load_weather_data',
        python_callable = main.load_data
    )

   
extract_data >> transform_data >> load_data


