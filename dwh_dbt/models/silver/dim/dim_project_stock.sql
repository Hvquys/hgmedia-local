-- silver.dim_project_stock  (target theo Data Dictionary)
select
    -- dim_project_stock.sql
{{ dbt_utils.generate_surrogate_key(['"Id"']) }} as dim_project_stock_sk
, nullif(trim(cast("Id" as text)),'') as project_id
, nullif(trim(cast("ProjectName" as text)),'') as project_name
from {{ source('staging','project') }}
where "ParentId" is null   -- theo dictionary: Project chính = ParentId IS NULL