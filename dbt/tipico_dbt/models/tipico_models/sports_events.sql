{{ config(
    materialized='table',
    unique_key='id',
    
) }}

with sports_events as (
    select 
        id, --primarykey unique
        startTime, 
        messageTime,
        sportType,
        matchState,
        status,
        marketCount,
        eventType ,
        updatesCount,
        eventReferences,
        eventName,
        lastModifiedTime,
        eventTags,
        score,
        gameClock,
        sportCurrentGameState,
        eventMetadata
    from vrashi_shrivastava.events
    
)

select * from sports_events