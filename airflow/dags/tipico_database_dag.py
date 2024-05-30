
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import csv
from datetime import datetime
from tipico_databse import create_database_redshift


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
    'tipico_database_dag',
    default_args=default_args,
    description='DAG to fetch Tipico API data every 10 minutes',
    schedule_interval=None,
    catchup=False,
)

# Define the task
creat_database = PythonOperator(
    task_id='create_database_redshift',
    python_callable=create_database_redshift,
    dag=dag,
)

# Set the task
creat_database
