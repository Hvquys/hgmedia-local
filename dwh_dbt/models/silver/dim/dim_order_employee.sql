-- silver.dim_order_employee  (target theo Data Dictionary — nguồn odoo.hr_employee)
select
    {{ dbt_utils.generate_surrogate_key(['he.id']) }} as dim_order_employee_sk
    , nullif(trim(cast(he.user_id as text)),'') as order_employee_id
    , nullif(trim(cast(hd.id as text)),'') as department_id
    , nullif(trim(cast(he.name as text)),'') as employee_name
    , null as team_name
    , nullif(trim(coalesce(
        cast(hj.name as jsonb) ->> 'vi_VN',
        cast(hj.name as jsonb) ->> 'en_US'
    )),'') as position
from {{ source('staging', 'hr_employee') }} he
left join {{ source('staging', 'hr_department') }} hd on he.department_id = hd.id
left join {{ source('staging', 'hr_job') }} hj on he.job_id = hj.id