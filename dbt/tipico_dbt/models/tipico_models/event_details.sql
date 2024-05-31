{{ config(
    materialized='table',
    unique_key='id',
    
) }}

with event_details as (
    select 
        id as event_id, --primarykey unique
        eventDetails_block_cashout,
        eventDetails_longTermEventType,
        eventDetails_outrightType,
        eventDetails_best_of_sets,
        eventDetails_activeParticipant,
        eventDetails_subgroupIdKey, --fk to subgroup table
        eventDetails_tiebreak,
        eventDetails_home_pitcher,
        eventDetails_away_pitcher
    from vrashi_shrivastava.events
    
)

select * from event_details