{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}


select
{{ dbt_utils.generate_surrogate_key(['emailid', 'emailname']) }} as emailkey,
emailid,
emailname
FROM {{ source('emailevents_landing', 'marketingemails') }}