{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}


select
{{ dbt_utils.generate_surrogate_key(['customer_id', 'customer_first_name', 'customer_last_name']) }} as custkey,
customer_id,
customer_first_name,
customer_last_name,
customer_phone,
customer_address,
customer_city,
customer_state,
customer_zip,
customer_country,
customer_email,
subscriberid
FROM {{ source('emailevents_landing', 'marketingemails') }} m
    JOIN {{ source('transactional_landing', 'customer') }} c
        ON m.subscriberfirstname = c.customer_first_name AND m.subscriberlastname = c.customer_last_name
