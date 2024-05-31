-- models/outcomes.sql

{{ config(
    materialized='table',
    unique_key='id'
) }}

with outcome as (
    select
        outcome.id,
        outcome.name,
        outcome.isTraded,
        outcome.formatDecimal,
        outcome.formatAmerican,
        outcome.status,
        outcome.trueOdds,
        market.event_id as market_id
    from {{ ref('markets') }} as market,
         unnest(market.outcome) as outcome  -- Assuming 'outcomes' is an array of JSON objects
)

select * from outcomes
