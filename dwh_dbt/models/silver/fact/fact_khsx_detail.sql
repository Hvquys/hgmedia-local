-- silver.fact_khsx_detail  (target theo Data Dictionary)
{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['id']) }} as fact_khsx_detail_sk
    , nullif(trim(cast(id as text)),'') as production_plan_detail_id
    , nullif(trim(cast(plan_id as text)),'') as production_plan_id
    , nullif(trim(cast(produce_manager as text)),'') as production_manager
    , nullif(trim(cast(ar_id as text)),'') as ar_manager
    , nullif(trim(cast(state as text)),'') as status
from {{ source('staging','x_music_plan_detail') }}
