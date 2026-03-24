{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}

with cte_date as (
{{ dbt_date.get_date_dimension("1990-01-01", "2050-12-31") }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as datekey,
    date_day as fulldate,
    day_of_week as dayofweek,
    month_name as month,
    year_number as year,
    from cte_date