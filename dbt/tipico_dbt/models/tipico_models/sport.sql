-- models/category.sql
{{ config(
    materialized='table',
    unique_key='group_parentGroup_parentGroup_id',
    
) }}

with sport as (
    select distinct
        group_parentGroup_parentGroup_id as sport_id,
        group_parentGroup_parentGroup_name as sport_name
    from vrashi_shrivastava.events
  
)

select * from sport