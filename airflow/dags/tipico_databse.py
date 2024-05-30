import requests
#from airflow.utils.dates import days_ago
import csv
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from datetime import datetime
import pandas as pd
import os

def create_database_redshift():
    
    # Create RedshiftSQLHook
    redshift_hook = RedshiftSQLHook(redshift_conn_id='redshift_default')
    conn = redshift_hook.get_conn()
    
    # Convert DataFrame to CSV and upload to Redshift
    cursor = conn.cursor()
    
    sql = f"""
    CREATE TABLE EVENTS (
    id int PRIMARY KEY,
    startTime TIMESTAMPTZ,
    messageTime TIMESTAMPTZ,
    sportType varchar(255),
    matchState varchar(255),
    participants varchar(10000),
    status varchar(255),
    marketCount int,
    groupDetails varchar(10000), -- "group is a reserved kayword hence chaging the column name"
    markets varchar(10000),
    eventType varchar(255),
    updatesCount int,
    displayInfo varchar(255),
    score varchar(10000),
    gameClock varchar(10000),
    eventReferences varchar(10000),
    eventDetails varchar(10000),
    sportCurrentGameState varchar(10000),
    lastModifiedTime TIMESTAMPTZ,
    eventTags varchar(10000),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
        """
    cursor.execute(sql)
    conn.commit()