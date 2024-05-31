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
    status varchar(255),
    marketCount int,
    eventType varchar(255),
    updatesCount int,
    displayInfo varchar(255),
    eventReferences varchar(max),
    eventName varchar(max),
    lastModifiedTime TIMESTAMPTZ,
    eventTags varchar(max),
    score varchar(max),
    gameClock varchar(max),
    sportCurrentGameState varchar(max),
    eventMetadata varchar(max),
    participant_id int,
    participant_name varchar(1000),
    participant_position varchar(255),
    participant_abbreviation varchar(3),
    eventDetails_block_cashout boolean,
    eventDetails_longTermEventType varchar(255),
    eventDetails_outrightType varchar(255),
    eventDetails_subgroupNameKey varchar(500),
    eventDetails_best_of_sets int,
    eventDetails_activeParticipant varchar(255),
    eventDetails_subgroupIdKey int,
    eventDetails_tiebreak varchar(255),
    eventDetails_home_pitcher varchar(255),
    eventDetails_away_pitcher varchar(255),
    group_id int,
    group_name varchar(255),
    group_parentGroup_id int,
    group_parentGroup_name varchar(255),
    group_parentGroup_parentGroup_id int,
    group_parentGroup_parentGroup_name varchar(255),
    markets_id varchar(255),
    markets_name varchar(255),
    markets_type varchar(255),
    markets_parameters varchar(255),
    markets_status varchar(255),
    markets_mostBalancedLine boolean,
    markets_sgpEligable boolean,
    markets_outcomes varchar(max),
    market_specifier varchar(max)
    );
        """
    cursor.execute(sql)
    conn.commit()