{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}

with sales_source as (
    select
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_phone,
    customer_address,
    customer_city,
    customer_state,
    customer_zip,
    customer_country,
    customer_email
    FROM {{ source('transactional_landing', 'customer') }}
)

, cs_interactions_source as (
    select distinct 
        subscriberid,
        subscriberfirstname,
        subscriberlastname,
        subscriberemail
        from {{ source('emailevents_landing', 'marketingemails') }}
)
, final as (
    select 
    s.customer_id,
    coalesce(s.customer_first_name, cs.subscriberfirstname) as firstname,
    coalesce(s.customer_last_name, cs.subscriberlastname) as lastname,
    s.customer_phone,
    s.customer_address,
    s.customer_city,
    s.customer_state,
    s.customer_zip,
    s.customer_country,
    coalesce(s.customer_email, cs.subscriberemail) as email,
    cs.subscriberid
    from sales_source s
    full join cs_interactions_source cs
    on s.customer_first_name = cs.subscriberfirstname
    and s.customer_last_name = cs.subscriberlastname
)

select 
    {{ dbt_utils.generate_surrogate_key(['firstname', 'lastname'])}} as custkey,
    *
from final