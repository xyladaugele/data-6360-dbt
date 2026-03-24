{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['eventtimestamp']) }} as timekey,
    cast(eventtimestamp as time) as timestamp,
    extract(hour from eventtimestamp) as hour,
    extract(minute from eventtimestamp) as minute,
    extract(second from eventtimestamp) as second

FROM {{ source('emailevents_landing', 'marketingemails') }}