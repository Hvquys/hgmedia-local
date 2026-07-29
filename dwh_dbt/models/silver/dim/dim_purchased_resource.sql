-- silver.dim_purchased_resource
-- select distinct on (pr."Mã")
--     {{ dbt_utils.generate_surrogate_key(['pr."Mã"']) }} as dim_purchased_resource_sk
--     , nullif(trim(pr."Mã"), '') as hg_stock_id
--     , nullif(trim(pr."Tiêu đề"), '') as resources_name
--     , nullif(trim(pr."Repository"), '') as repository
--     , {{ dbt_utils.generate_surrogate_key(['pr."Nghệ sĩ bài hát"']) }} as partner_id
--     , nullif(trim(p."Ngày ký HĐ"), '') as buy_date
--     , nullif(trim(p."Cách tính giá"), '') as repository_type
-- from {{ source('staging', 'purchased_resource') }} pr
-- left join {{ source('staging', 'partners') }} p
--     on trim(pr."Repository") = trim(p."Tên kho trên HG Stock")
-- where trim(pr."Mã") ~ '^HG[A-F0-9]+$'
-- order by pr."Mã"

-- silver.dim_purchased_resource
-- silver.dim_purchased_resource
-- silver.dim_purchased_resource
-- silver.dim_purchased_resource

select distinct on (pr."Mã")
    {{ dbt_utils.generate_surrogate_key(['pr."Mã"']) }} as dim_purchased_resource_sk
    , nullif(trim(pr."Mã"), '') as hg_stock_id
    , nullif(trim(pr."Tiêu đề"), '') as resources_name
    , dr.repository_id as repository
    , {{ dbt_utils.generate_surrogate_key(['pr."Nghệ sĩ bài hát"']) }} as partner_id
    , nullif(trim(p."Ngày ký HĐ"), '') as buy_date
    , nullif(trim(p."Cách tính giá"), '') as repository_type
    , dsp.sub_project_id
from {{ source('staging', 'purchased_resource') }} pr
left join {{ source('staging', 'partners') }} p
    on trim(pr."Repository") = trim(p."Tên kho trên HG Stock")
left join {{ ref('dim_repository') }} dr
    on nullif(trim(pr."Repository"), '') = dr.repository_name
left join {{ ref('dim_project') }} dp
    on nullif(trim(p."Dự án"), '') = dp.project_name
left join {{ ref('dim_sub_project') }} dsp
    on dp.project_id = dsp.project_id
    and dsp.sub_project_name = 'Không có dự án con'
where trim(pr."Mã") ~ '^HGFA[A-F0-9]+$'
order by pr."Mã"