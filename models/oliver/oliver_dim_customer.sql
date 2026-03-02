{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}


select
customer_id as customer_key,
customer_id
email,
state, 
last_name,
first_name,
phone_number
FROM {{ source('oliver_landing', 'customer') }}