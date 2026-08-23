-- silver.dim_company  (target theo Data Dictionary — nguồn odoo.res_company)
select
    {{ dbt_utils.generate_surrogate_key(['id']) }} as dim_company_sk
    , nullif(trim(cast(id as text)),'')   as company_id
    , nullif(trim(cast(name as text)),'') as company_name
from {{ source('staging', 'res_company') }}