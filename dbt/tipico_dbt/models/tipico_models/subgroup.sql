-- models/category.sql
{{ config(
    materialized='table',
    unique_key='eventDetails_subgroupIdKey',
    
) }}

with subgroup as (
    select distinct
        eventDetails_subgroupIdKey as subgroup_id,
        eventDetails_subgroupNameKey as subgroup_name
    from vrashi_shrivastava.events
  
)

select * from subgroup