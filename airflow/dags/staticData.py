from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from pendulum import datetime
#from planetData import get_planet_data
import pandas as pd
import sys
from SLTrack import api_call, load_satellite_data

default_args = {
    'owner':'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022,6,15),
    'retries': 1
}

dag = DAG(
    'Sattellite Static Data',
    default_args=default_args,
    description='Makes an api request to space track to get gp data',
    schedule_interval=timedelta(days=1)
)


api = PythonOperator(
    task_id='api_request',
    python_callable=api_call,
    dag=dag
)

load = PythonOperator(
    task_id='load_data',
    python_callable=load_satellite_data,
    dag=dag
)

load