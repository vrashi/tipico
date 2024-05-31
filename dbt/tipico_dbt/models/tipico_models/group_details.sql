-- models/league.sql
{{ config(
    materialized='table',
    unique_key='id',
    
) }}

with group_details as (
    select distinct
        id as groupid, -- fk to sports_events table
        group_id as league_id, -- fk to league table
        group_parentGroup_id as category_id, -- fk to category table
        group_parentGroup_parentGroup_id as sport_id -- fk to sport table
    from vrashi_shrivastava.events
  
)

select * from group_details