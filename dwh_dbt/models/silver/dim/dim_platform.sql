-- silver.dim_platform  (target theo Data Dictionary)
select
    {{ dbt_utils.generate_surrogate_key(['retailer']) }} as dim_platform_sk
    , {{ dbt_utils.generate_surrogate_key(['retailer']) }} as artist_id
    , nullif(trim(retailer),'') as artist_name
from {{ source('staging','sale') }}
where nullif(trim(retailer),'') is not null
group by retailer