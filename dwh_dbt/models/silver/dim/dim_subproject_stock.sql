-- silver.dim_subproject_stock  (target theo Data Dictionary)
select
    -- dim_subproject_stock.sql
{{ dbt_utils.generate_surrogate_key(['"Id"']) }} as dim_subproject_stock_sk
, nullif(trim(cast("Id" as text)),'') as sub_project_id
, nullif(trim(cast("ProjectName" as text)),'') as sub_project_name
from {{ source('staging','project') }}
where "ParentId" is not null   -- Sub-project = có ParentId