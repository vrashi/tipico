import requests
#from airflow.utils.dates import days_ago
import csv
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from datetime import datetime
import pandas as pd
import os

def fetch_tipico_data():

    url = 'https://trading-api.tipico.us/v1/pds/lbc/events/live?licenseId=US-NJ&lang=en&limit=18'
    response = requests.get(url)
    data = response.json()

    df = pd.DataFrame(data)

    filename = f'./data/tipico_data_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'

    df.to_csv(filename)
    
#run_tipico_etl()

def load_data_to_redshift():
    
    # Create RedshiftSQLHook
    redshift_hook = RedshiftSQLHook(redshift_conn_id='redshift_default')
    conn = redshift_hook.get_conn()
    
    # Convert DataFrame to CSV and upload to Redshift
    cursor = conn.cursor()
    for index, row in df.iterrows():
        sql = f"""
        INSERT INTO vrashi_shrivastava.events (column1, column2, column3)  -- Adjust columns based on your table structure
        VALUES ({row['column1']}, '{row['column2']}', '{row['column3']}');
        """
        cursor.execute(sql)
    conn.commit()