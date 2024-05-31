
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import csv
from datetime import datetime
from tipico_etl import fetch_tipico_data
from tipico_etl import load_data_to_redshift


# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'tipico_dag',
    default_args=default_args,
    description='DAG to fetch Tipico API data every 10 minutes',
    schedule_interval='*/10 * * * *',
    catchup=False,
)

# Define the task
fetch_data = PythonOperator(
    task_id='fetch_tipico_data',
    python_callable=fetch_tipico_data,
    dag=dag,
)
load_data = PythonOperator(
    task_id='load_data_to_redshift',
    python_callable=load_data_to_redshift,
    dag=dag,
)

# Set the task
fetch_data >> load_data
