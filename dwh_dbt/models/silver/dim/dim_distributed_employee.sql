-- silver.dim_distributed_employee  (target theo Data Dictionary)
select
    {{ dbt_utils.generate_surrogate_key(['"Id"']) }} as dim_distributed_employee_sk
    , nullif(trim(cast("Id" as text)),'') as emp_id
    , nullif(trim(cast("DisplayName" as text)),'') as emp_name
    
    , null as department_id   -- TODO user_departments.DepartmentEnum | Department ID (INT) - join với users.Id = user_departments.UserId
    , null as team   -- TODO ? | 
    , null as role   -- TODO roles.Name | 1:1
from {{ source('staging', 'users') }}

-- left join {{ source('staging','user_departments') }} on ...  -- Department ID (INT) - join với users.Id = user_departments.UserId
-- left join {{ source('staging','roles') }} on ...  -- 1:1
