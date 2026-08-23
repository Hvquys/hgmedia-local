-- silver.dim_ar  (target theo Data Dictionary)
select
    {{ dbt_utils.generate_surrogate_key(['id']) }} as dim_ar_sk
    , nullif(trim(cast(id as text)),'') as "mã a&r"
    , nullif(trim(cast(name as text)),'') as "tên a&r"
from {{ source('staging', 'hr_employee') }}