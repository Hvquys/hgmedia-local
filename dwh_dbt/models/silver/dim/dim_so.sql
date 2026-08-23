-- silver.dim_so  (target theo Data Dictionary)
select
    {{ dbt_utils.generate_surrogate_key(['sol.id']) }} as dim_so_sk
    , nullif(trim(cast(sol.id as text)), '') as so_id
    , nullif(trim(cast(so.name as text)), '') as so_name
    , nullif(trim(cast(sol.x_purchase_line_id as text)), '') as po_id
    , cast(nullif(trim(cast(sol.create_date as text)), '') as timestamp) as so_created_date
    , cast(nullif(trim(cast(sol.write_date as text)), '') as timestamp) as so_confirmed_date
    , nullif(trim(cast(sol.state as text)), '') as status
from {{ source('staging', 'sale_order_line') }} sol
left join {{ source('staging', 'sale_order') }} so
    on sol.order_id = so.id