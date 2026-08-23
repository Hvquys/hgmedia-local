-- silver.dim_partners
select distinct on ("Tên đối tác")
    {{ dbt_utils.generate_surrogate_key(['"Tên đối tác"']) }} as partner_id
    , nullif(trim("Tên đối tác"), '') as partner_name
from {{ source('staging', 'partners') }}
where nullif(trim("Tên đối tác"), '') is not null
order by "Tên đối tác"