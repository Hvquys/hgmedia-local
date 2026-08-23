-- silver.dim_department  (target theo Data Dictionary — nguồn odoo.hr_department)
-- select
--     {{ dbt_utils.generate_surrogate_key(['hd.id']) }} as dim_department_sk
--     , nullif(trim(cast(hd.hg_code as text)),'') as department_id
--     , nullif(trim(cast(rc.id as text)),'') as company_id
--     , nullif(trim(coalesce(
--         cast(hd.name as jsonb) ->> 'vi_VN',
--         cast(hd.name as jsonb) ->> 'en_US'
--       )),'') as department_name
-- from {{ source('staging', 'hr_department') }} hd
-- left join {{ source('staging', 'res_company') }} rc on hd.company_id = rc.id

select
    {{ dbt_utils.generate_surrogate_key(['hd.id']) }} as dim_department_sk
    , nullif(trim(cast(hd.id as text)),'')         as department_id
    , nullif(trim(cast(hd.company_id as text)),'') as company_id
, nullif(trim(coalesce(
        cast(hd.name as jsonb) ->> 'vi_VN',
        cast(hd.name as jsonb) ->> 'en_US'
      )),'') as department_name
from {{ source('staging', 'hr_department') }} hd