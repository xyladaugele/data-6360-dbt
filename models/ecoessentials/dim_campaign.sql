{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}


select
{{ dbt_utils.generate_surrogate_key(['campaignid', 'campaignname']) }} as campaignkey,
campaign_id,
campaignname,
campaign_discount
FROM {{ source('emailevents_landing', 'marketingemails') }} m
    LEFT JOIN {{ source('transactional_landing', 'promotional_campaign') }} p
        ON m.campaignid = p.campaign_id
