
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import csv
from datetime import datetime
from tipico_etl import fetch_tipico_data


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
run_etl = PythonOperator(
    task_id='run_tipico_etl',
    python_callable=fetch_tipico_data,
    dag=dag,
)

# Set the task
run_etl
