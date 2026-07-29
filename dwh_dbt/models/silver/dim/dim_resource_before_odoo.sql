-- silver.dim_resource_before_odoo
-- silver.dim_resource_before_odoo
select distinct on (trim("ISRC"))
    {{ dbt_utils.generate_surrogate_key(['"ISRC"']) }} as dim_resource_before_odoo_sk
    , nullif(trim("ISRC"), '') as isrc
    , nullif(trim("Mã bài"), '') as resource_name
    , cast(nullif(trim("Điểm trung bình"), '') as numeric(18,2)) as aceptance_score
    , cast(nullif(trim("Chi phí đv: $"), '') as numeric(18,2)) as aceptance_price
    , nullif(trim("A&R phụ trách"), '') as ar
    , nullif(trim("Nghệ sỹ"), '') as artist
    , dsp.sub_project_id
from {{ source('staging', 'resource_before_odoo') }} rbo
left join {{ ref('dim_sub_project') }} dsp
    on nullif(trim(rbo."Subgenre"), '') = dsp.sub_project_name
where nullif(trim("ISRC"), '') is not null
    and trim("ISRC") not in ('#N/A', '#REF!')
order by trim("ISRC")