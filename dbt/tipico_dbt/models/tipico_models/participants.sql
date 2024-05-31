-- models/participants.sql
{{ config(
    materialized='table',
    unique_key='participant_id',
    
) }}

with participants as (
    select 
        participant_name as name,
        participant_id,
        participant_position as position,
        participant_abbreviation as abbreviation,
        id as event_id --FK to sports_events table
    from vrashi_shrivastava.events
  
)

select * from participants



