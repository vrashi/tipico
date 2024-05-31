{{ config(
    materialized='table',
    unique_key='id',
    
) }}

with markets as (
    select 
        id as event_id, --fk to sports_events table
        markets_id, -- primary key unique
        markets_name,
        markets_type,
        markets_parameters,
        markets_status,
        markets_mostBalancedLine, 
        markets_sgpEligable
    from vrashi_shrivastava.events
    
)

select * from markets