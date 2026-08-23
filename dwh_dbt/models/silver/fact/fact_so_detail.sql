-- silver.fact_so_detail  (target theo Data Dictionary)
{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['id']) }} as fact_so_detail_sk
    , nullif(trim(cast(id as text)),'') as so_detail_id
    , nullif(trim(cast(order_id as text)),'') as so_id
    , nullif(trim(cast(x_subgenre_id as text)),'') as repository
    , cast(nullif(trim(cast(product_uom_qty as text)),'') as numeric) as song_qty
    , nullif(trim(cast(x_deadline as text)),'') as deadline
    , cast(nullif(trim(cast(x_expected_score as text)),'') as numeric(18,2)) as expected_score
    , cast(nullif(trim(cast(x_produce_qty as text)),'') as numeric) as production_qty
    , cast(nullif(trim(cast(x_qty_in_stock as text)),'') as numeric) as pending_qty
from {{ source('staging', 'sale_order_line') }}
