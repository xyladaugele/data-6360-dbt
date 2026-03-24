{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}


select
{{ dbt_utils.generate_surrogate_key(['eventtype']) }} as eventkey,
emaileventid,
eventtype
FROM {{ source('emailevents_landing', 'marketingemails') }}