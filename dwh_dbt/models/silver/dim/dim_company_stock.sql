-- silver.dim_company_stock  (target theo Data Dictionary)
with department_level as (
    select
        "Id"
        , "Name"
    from {{ source('staging', 'departmentlevel') }}
    where trim("Name") = 'Công ty'
)

, department as (
    select
        "Id"
        , "Name"
        , "DepartmentLevelId"
    from {{ source('staging', 'department') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['d."Id"']) }} as dim_company_stock_sk
    , nullif(trim(cast(d."Id" as text)),'')   as company_id
    , nullif(trim(cast(d."Name" as text)),'') as company_name
from department d
inner join department_level dl
    on d."DepartmentLevelId" = dl."Id"