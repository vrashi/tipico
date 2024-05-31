-- models/league.sql
{{ config(
    materialized='table',
    unique_key='league_id',
    
) }}

with league as (
    select distinct
        group_id as league_id,
        group_name as league_name
    from vrashi_shrivastava.events
  
)

select * from league



