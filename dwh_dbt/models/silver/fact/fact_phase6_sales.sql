{{ config(materialized='table') }}

with source_data as (

    select
        nullif(trim(sale_id), '') as sale_id,
        nullif(trim(customer_name), '') as customer_name,
        cast(nullif(trim(amount), '') as numeric(18, 2)) as amount,
        cast(nullif(trim(sale_date), '') as date) as sale_date,
        _source_id,
        _batch_id,
        _loaded_at
    from {{ source('staging', 'phase6_sales') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by sale_id
            order by _loaded_at desc
        ) as row_num
    from source_data
    where sale_id is not null

)

select
    sale_id,
    customer_name,
    amount,
    sale_date,
    _source_id,
    _batch_id,
    _loaded_at
from deduplicated
where row_num = 1