import requests
#from airflow.utils.dates import days_ago
import csv
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from datetime import datetime
import pandas as pd
import json
import os
from tipico_transform import get_eventDetails, get_group, get_market_outcome, get_markets, get_participants


#function to fetch the data from api
def fetch_tipico_data():

    url = 'https://trading-api.tipico.us/v1/pds/lbc/events/live?licenseId=US-NJ&lang=en&limit=18'
    response = requests.get(url)
    data = response.json()

    df = pd.DataFrame(data)

    filename = f'./data/tipico_data_{datetime.now().strftime("%Y%m%d%H%M")}.csv'

    df.to_csv(filename)
    
#function to load the fetched data into the redshift database

def load_data_to_redshift():

    #fetching the dumped archived data
    #filename = f'./data/tipico_data_{datetime.now().strftime("%Y%m%d%H%M")}.csv'
    filename = f'./data/test_transformation.csv' 
    df = pd.read_csv(filename)
    print(df.head())

    def replace_single_quotes(value):
        if isinstance(value, str):
            return value.replace("'", '"')
        return value
    
    df = df.applymap(replace_single_quotes)
    def replace_truefalse(value):
        if isinstance(value, str):
            value = value.replace("True", "true").replace("False", "false")
        return value
    df = df.applymap(replace_truefalse)
    

    df = get_participants(df)
    
    df = get_eventDetails(df)
    df = get_group(df)
    df = get_markets(df)
    df = df.applymap(replace_truefalse)
    df = df.applymap(replace_single_quotes)
    
    # Create RedshiftSQLHook
    redshift_hook = RedshiftSQLHook(redshift_conn_id='redshift_default')
    conn = redshift_hook.get_conn()

    #creating a default value dictionary for fault tolerance
    default_values = {
        'id':'NULL',
        'startTime': 'NULL',
        'messageTime': 'NULL',
        'sportType': 'NULL',
        'matchState': 'NULL',
        'status': 'NULL',
        'marketCount': 'NULL',
        'eventType': 'NULL',
        'updatesCount': 'NULL',
        'displayInfo': 'NULL',
        'eventReferences': 'NULL',
        'eventName': 'NULL',
        'lastModifiedTime': 'NULL',
        'eventTags': 'NULL',
        'score': 'NULL',
        'gameClock': 'NULL',
        'sportCurrentGameState': 'NULL',
        'eventMetadata': 'NULL',
        'participant_id': 'NULL',
        'participant_name': 'NULL',
        'participant_position': 'NULL',
        'participant_abbreviation': 'NULL',
        'eventDetails_block_cashout': 'NULL',
        'eventDetails_longTermEventType': 'NULL',
        'eventDetails_outrightType': 'NULL',
        'eventDetails_subgroupNameKey': 'NULL',
        'eventDetails_best_of_sets': 'NULL',
        'eventDetails_activeParticipant': 'NULL',
        'eventDetails_subgroupIdKey': 'NULL',
        'eventDetails_tiebreak': 'NULL',
        'group_id': 'NULL',
        'group_name': 'NULL',
        'group_parentGroup.id': 'NULL',
        'group_parentGroup.name': 'NULL',
        'group_parentGroup.parentGroup.id': 'NULL',
        'group_parentGroup.parentGroup.name': 'NULL',
        'markets_id' : 'NULL',
        'markets_name' : 'NULL',
        'markets_type' : 'NULL',
        'markets_parameters': 'NULL',
        'markets_status' : 'NULL',
        'markets_mostBalancedLine' : 'NULL',
        'markets_sgpEligable' : 'NULL'
    }
    # ingesting df into redshift
    cursor = conn.cursor()
    for index, row in df.iterrows():
        values = {}
        for col in default_values.keys():
            values[col] = row[col] if col in df.columns else default_values[col]
        
        sql = f"""INSERT INTO vrashi_shrivastava.EVENTS (
            id,
            startTime,
            messageTime,
            sportType,
            matchState,
            status,
            marketCount,
            eventType,
            updatesCount,
            displayInfo,
            eventReferences,
            eventName,
            lastModifiedTime,
            eventTags,
            score,
            gameClock,
            sportCurrentGameState,
            eventMetadata,
            participant_id,
            participant_name,
            participant_position,
            participant_abbreviation,
            eventDetails_block_cashout,
            eventDetails_longTermEventType,
            eventDetails_outrightType,
            eventDetails_subgroupNameKey,
            eventDetails_best_of_sets,
            eventDetails_activeParticipant,
            eventDetails_subgroupIdKey,
            eventDetails_tiebreak,
            group_id,
            group_name,
            group_parentGroup_id,
            group_parentGroup_name,
            group_parentGroup_parentGroup_id,
            group_parentGroup_parentGroup_name,
            markets_id,
            markets_name,
            markets_type,
            markets_parameters,
            markets_status,
            markets_mostBalancedLine,
            markets_sgpEligable
            ) 
        VALUES(
            {values['id']},
            '{values['startTime']}',
            '{values['messageTime']}',
            '{values['sportType']}',
            '{values['matchState']}',
            '{values['status']}',
            {values['marketCount']},
            '{values['eventType']}',
            {values['updatesCount']},
            '{values['displayInfo']}',
            '{values['eventReferences']}',
            '{values['eventName']}',
            '{values['lastModifiedTime']}',
            '{values['eventTags']}',
            '{values['score']}',
            '{values['gameClock']}',
            '{values['sportCurrentGameState']}',
            '{values['eventMetadata']}',
            {values['participant_id']},
            '{values['participant_name']}',
            '{values['participant_position']}',
            '{values['participant_abbreviation']}',
            {values['eventDetails_block_cashout']},
            '{values['eventDetails_longTermEventType']}',
            '{values['eventDetails_outrightType']}',
            '{values['eventDetails_subgroupNameKey']}',
            {values['eventDetails_best_of_sets']},
            '{values['eventDetails_activeParticipant']}',
            {values['eventDetails_subgroupIdKey']},
            '{values['eventDetails_tiebreak']}',
            {values['group_id']},
            '{values['group_name']}',
            {values['group_parentGroup.id']},
            '{values['group_parentGroup.name']}',
            {values['group_parentGroup.parentGroup.id']},
            '{values['group_parentGroup.parentGroup.name']}',
           '{values['markets_id']}',
           '{values['markets_name']}',
            '{values['markets_type']}',
            '{values['markets_parameters']}',
            '{values['markets_status']}',
            '{values['markets_mostBalancedLine']}',
            '{values['markets_sgpEligable']}'
        );
        """
        cursor.execute(sql)
    conn.commit()