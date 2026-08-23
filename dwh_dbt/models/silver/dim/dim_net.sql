-- silver.dim_net  (target theo Data Dictionary)
select
    {{ dbt_utils.generate_surrogate_key(['"Id"']) }} as dim_net_sk
    , nullif(trim(cast("Id" as text)),'') as net_id
    , nullif(trim(cast("Name" as text)),'') as net_name
from {{ source('staging', 'network') }}
