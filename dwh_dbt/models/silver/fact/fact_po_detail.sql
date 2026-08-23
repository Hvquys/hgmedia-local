-- silver.fact_po_detail  (target theo Data Dictionary)
{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['id']) }} as fact_po_detail_sk
    , nullif(trim(cast(id as text)),'') as po_detail_id
    , nullif(trim(cast(order_id as text)),'') as po_id
    , nullif(trim(cast(product_qty as text)),'') as qty_order
    , nullif(trim(cast(x_subgenre_id as text)),'') as repository
from {{ source('staging', 'purchase_order_line') }}
