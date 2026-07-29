{{ config(materialized='table', schema='silver') }}

select distinct
    fe.hg_stock_id
    , b.video_id
from {{ ref('fact_editing') }} fe
join {{ ref('bridge_bt_vid') }} b on fe.editing_code = b.editing_code