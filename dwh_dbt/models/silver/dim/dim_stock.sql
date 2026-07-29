with base as (
    select
        nullif(trim(cast(rf."Id" as text)), '') as hg_stock_id,
        rf."FileName" as name,
        nullif(trim(rfi."ISRC"), '') as isrc,
        rf."CreatedDate" as created
    from {{ source('staging', 'resource_files') }} rf
    left join {{ source('staging', 'resource_file_info') }} rfi
        on nullif(trim(cast(rf."Id" as text)), '')
         = nullif(trim(cast(rfi."ResourceFileId" as text)), '')
    where
        nullif(trim(cast(rf."Id" as text)), '') is not null
        and rf."MediaType" = 1
),

from_excel as (
    select
        nullif(trim("Mã Stock"), '') as hg_stock_id
        , nullif(trim("Tên bài"), '') as name
        , coalesce(
            nullif(trim("ISRC chốt"), ''),
            nullif(trim("ISRC (Stock cũ)"), ''),
            nullif(trim("ISRC (Stock mới)"), '')
          ) as isrc
        , cast(null as timestamp) as created
    from {{ source('staging', 'resource_performance') }}
    where nullif(trim("Mã Stock"), '') is not null
        and trim("Mã Stock") ~ '^HGFA[A-F0-9]+$'
        and nullif(trim("Ưu tiên"), '') is not null
        and nullif(trim("Mã Stock"), '') not in (
            select hg_stock_id from base where hg_stock_id is not null
        )
),

combined as (
    select * from base
    union all
    select * from from_excel
),

dedup_by_stock as (
    select distinct on (hg_stock_id)
        hg_stock_id,
        name,
        isrc,
        created
    from combined
    order by
        hg_stock_id,
        case when name not ilike '%.wav' then 0 else 1 end,
        created asc nulls last
),

vid as (
    select
        f.hg_stock_id,
        max(cast(v.published_date as date)) as last_published
    from {{ ref('fact_editing') }} f
    join {{ ref('bridge_bt_vid') }} b on f.editing_code = b.editing_code
    join {{ ref('dim_video') }} v on b.video_id = v.video_id
    where v.published_date is not null
    group by f.hg_stock_id
),

edited as (
    select distinct hg_stock_id
    from {{ ref('fact_editing') }}
),

archived as (
    select distinct
        nullif(trim(cast(rf."Id" as text)), '') as hg_stock_id
    from {{ source('staging', 'resource_storage_history') }} h
    join {{ source('staging', 'resource_file_info') }} rfi
        on nullif(trim(cast(h."ResourceFileInfoId" as text)), '')
         = nullif(trim(cast(rfi."Id" as text)), '')
    join {{ source('staging', 'resource_files') }} rf
        on nullif(trim(cast(rfi."ResourceFileId" as text)), '')
         = nullif(trim(cast(rf."Id" as text)), '')
    where
        h."ToStatus" = 'Archived'
        and rf."MediaType" = 1
        and nullif(trim(cast(rf."Id" as text)), '') is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['s.hg_stock_id']) }} as dim_stock_sk,
    s.hg_stock_id,
    nullif(trim(cast(s.name as text)), '') as name,
    s.isrc,
    cast(s.created as timestamp) as stock_stored_date,
    (current_date - cast(s.created as date)) as resource_age_days,
    case
        when a.hg_stock_id is not null then 'Lưu kho'
        when v.hg_stock_id is null then 'Tồn kho'
        when v.last_published < current_date - 30 then 'Hàng nguội'
        else 'Sử dụng'
    end as status
from dedup_by_stock s
left join vid v on s.hg_stock_id = v.hg_stock_id
left join archived a on s.hg_stock_id = a.hg_stock_id