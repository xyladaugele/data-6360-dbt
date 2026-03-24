{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}


select
{{ dbt_utils.generate_surrogate_key(['product_id', 'product_type']) }} as productkey,
product_id,
product_type,
product_name
FROM {{ source('transactional_landing', 'product') }}