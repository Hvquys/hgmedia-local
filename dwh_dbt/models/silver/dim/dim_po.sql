-- silver.dim_po  (target theo Data Dictionary)
select
    {{ dbt_utils.generate_surrogate_key(['id']) }} as dim_po_sk
    , nullif(trim(cast(id as text)),'') as po_id
    , cast(nullif(trim(cast(create_date as text)),'') as timestamp) as po_created_date
    , nullif(trim(cast(user_id  as text)),'') as order_employee_id
    , nullif(trim(cast(company_id as text)),'') as ordering_company
    , nullif(trim(cast(state as text)),'') as status
    , cast(nullif(trim(cast(date_approve as text)),'') as timestamp) as po_confirmed_date
    , nullif(trim(cast(name as text)),'') as po_name
from {{ source('staging', 'purchase_order') }}