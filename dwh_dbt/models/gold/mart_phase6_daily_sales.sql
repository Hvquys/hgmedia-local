{{ config(materialized='table') }}

select
    sale_date,
    count(*) as sale_count,
    sum(amount) as total_amount
from {{ ref('fact_phase6_sales') }}
group by sale_date