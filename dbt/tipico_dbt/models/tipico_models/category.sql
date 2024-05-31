-- models/category.sql
{{ config(
    materialized='table',
    unique_key='group_parentGroup_id',
    
) }}

with category as (
    select distinct
        group_parentGroup_id as category_id,
        group_parentGroup_name as category_name
    from vrashi_shrivastava.events
  
)

select * from category