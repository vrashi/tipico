import requests
#from airflow.utils.dates import days_ago
import csv
from datetime import datetime
import pandas as pd
import json
import os

def get_participants(df):
    combined_list = []
    #denormalizing participants
    for index, row in df.iterrows():
        if isinstance(row["participants"], str):
            list_in_cell = json.loads(row["participants"])
        else:
            list_in_cell = row["participants"]
        for dictionary in list_in_cell:
            dictionary['match_id'] = df['id'][index]
            combined_list.extend(list_in_cell)
    participant_df = pd.DataFrame(combined_list)
    #removing duplicates
    participant_df = participant_df.drop_duplicates(subset=['id', 'match_id'], keep = 'last')
    #merging participants
    participant_df = participant_df.rename(columns={col: f'participant_{col}' for col in participant_df.columns if col != 'match_id'}) # Merge the new DataFrame with the original DataFrame 
    merged_df = df.drop(columns=['participants']).merge(participant_df, left_on='id', right_on='match_id') # Drop the 'match_id' column if it's no longer needed 
    merged_df = merged_df.drop(columns=['match_id']) 
    
    return merged_df


def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def get_eventDetails(df):
    df['eventDetails'] = df['eventDetails'].apply(json.loads)

    event_details_flattened = pd.json_normalize(df['eventDetails'])
    event_details_flattened = event_details_flattened.rename(columns={col: f'eventDetails_{col}' for col in event_details_flattened.columns}) 
    df = df.drop(columns=['eventDetails']).join(event_details_flattened)
    return df


def get_group(df):
    df['group'] = df['group'].apply(json.loads)

    group_flattened = pd.json_normalize(df['group'])
    group_flattened = group_flattened.rename(columns={col: f'group_{col}' for col in group_flattened.columns}) 
    df = df.drop(columns=['group']).join(group_flattened)
    return df
    
def get_markets(df):
    try:
        df['markets'] = df['markets'].apply(json.loads)
    except json.JSONDecodeError as e:
        for i, row in df.iterrows():
            try:
                json.loads(row['markets'])
            except json.JSONDecodeError as inner_e:
                print(f"Row {i} caused error: {inner_e}")
                print(f"Problematic data: {row['markets']}")
    markets_flattened = pd.json_normalize(df['markets'])
    markets_flattened_more = pd.json_normalize(markets_flattened[0])
    markets_flattened_more = markets_flattened_more.rename(columns={col: f'markets_{col}' for col in markets_flattened_more.columns}) 
    df = df.drop(columns=['markets']).join(markets_flattened_more)
    return df

def get_market_outcome(df):
    try:
        df['markets_outcomes'] = df['markets_outcomes'].apply(json.loads)
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError: {e}")
        for i, row in df.iterrows():
            try:
                json.loads(row['markets_outcomes'])
            except json.JSONDecodeError as inner_e:
                print(f"Row {i} caused error: {inner_e}")
                print(f"Problematic data: {row['markets_outcomes']}")
    markets_outcomes_flattened = pd.json_normalize(df['markets_outcomes'])
    #need to flatten further

