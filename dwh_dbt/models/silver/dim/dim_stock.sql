-- Replacement model for dim_stock.
-- Sources: stored files, performance, RBO, purchased_resource,
-- and resource_infomation_add. Odoo x_music_song is intentionally excluded.

{{ config(on_schema_change='append_new_columns') }}

with excluded_stock as (
    select distinct
        upper(trim(cast(hg_stock_id as text))) as hg_stock_id
    from {{ ref('int_excluded_stock_codes') }}
    where nullif(trim(cast(hg_stock_id as text)), '') is not null
),

base as (
    select
        nullif(trim(cast(rf."Id" as text)), '') as hg_stock_id
        , rf."FileName" as name
        , nullif(trim(rfi."ISRC"), '') as isrc
        , rf."CreatedDate" as created
        , case
            when nullif(trim(cast(rfi."Id" as text)), '') is not null then
                'https://stock-admin.hgmedia.app/admin/resource/detail/'
                || '00000000-0000-0000-0000-000000000000/audio/grid/'
                || trim(cast(rfi."Id" as text))
                || '/0'
          end as stock_link
        , 1 as source_priority
    from {{ source('staging', 'resource_files') }} rf
    left join {{ source('staging', 'resource_file_info') }} rfi
        on nullif(trim(cast(rf."Id" as text)), '')
         = nullif(trim(cast(rfi."ResourceFileId" as text)), '')
    where nullif(trim(cast(rf."Id" as text)), '') is not null
        and rf."MediaType" = 1
),

from_performance as (
    select
        nullif(trim("Mã Stock"), '') as hg_stock_id
        , nullif(trim("Tên bài"), '') as name
        , coalesce(
            nullif(trim("ISRC chốt"), '')
            , nullif(trim("ISRC (Stock cũ)"), '')
            , nullif(trim("ISRC (Stock mới)"), '')
          ) as isrc
        , cast(null as timestamp) as created
        , cast(null as text) as stock_link
        , 2 as source_priority
    from {{ source('staging', 'resource_performance') }}
    where nullif(trim("Mã Stock"), '') is not null
        and trim("Mã Stock") ~ '^HGFA[A-F0-9]+$'
        and nullif(trim("Dự án chốt"), '') is not null
        and upper(trim("Dự án chốt")) <> '#N/A'
        and lower(normalize(trim("Dự án chốt"), nfc)) <> 'không xác định'
),

from_resource_before_odoo as (
    select
        nullif(trim(rbo."HG_Stock_ID"), '') as hg_stock_id
        , nullif(trim(rbo."Tên bài gốc"), '') as name
        , case
            when trim(rbo."ISRC") in ('#N/A', '#REF!') then null
            else nullif(trim(rbo."ISRC"), '')
          end as isrc
        , cast(null as timestamp) as created
        , cast(null as text) as stock_link
        , 3 as source_priority
    from {{ source('staging', 'resource_before_odoo') }} rbo
    where nullif(trim(rbo."HG_Stock_ID"), '') is not null
        and trim(rbo."HG_Stock_ID") ~ '^HGFA[A-F0-9]+$'
),

from_purchased_resource as (
    select
        nullif(trim(cast(pr."Mã" as text)), '') as hg_stock_id
        , nullif(trim(cast(pr."Tiêu đề" as text)), '') as name
        , cast(null as text) as isrc
        , cast(null as timestamp) as created
        , cast(null as text) as stock_link
        , 4 as source_priority
    from {{ source('staging', 'purchased_resource') }} pr
    where nullif(trim(cast(pr."Mã" as text)), '') is not null
        and trim(cast(pr."Mã" as text)) ~ '^HGFA[A-F0-9]+$'
),

from_resource_information_add as (
    select
        nullif(trim(cast(resource_add."ResourceFileId" as text)), '') as hg_stock_id
        , nullif(trim(cast(resource_add."Tiêu đề" as text)), '') as name
        , nullif(trim(cast(resource_add."ISRC" as text)), '') as isrc
        , cast(null as timestamp) as created
        , cast(null as text) as stock_link
        , 5 as source_priority
    from {{ source('staging', 'resource_infomation_add') }} resource_add
    where nullif(trim(cast(resource_add."ResourceFileId" as text)), '') is not null
        and trim(cast(resource_add."ResourceFileId" as text)) ~ '^HGFA[A-F0-9]+$'
),

combined as (
    select * from base

    union all

    select * from from_performance

    union all

    select * from from_resource_before_odoo

    union all

    select * from from_purchased_resource

    union all

    select * from from_resource_information_add
),

combined_filtered as (
    select c.*
    from combined c
    where not exists (
        select 1
        from excluded_stock e
        where e.hg_stock_id = upper(trim(cast(c.hg_stock_id as text)))
    )
),

dedup_by_stock as (
    select distinct on (hg_stock_id)
        hg_stock_id
        , name
        , isrc
        , created
        , stock_link
    from combined_filtered
    order by
        hg_stock_id
        , source_priority
        , case when name not ilike '%.wav' then 0 else 1 end
        , created asc nulls last
),

vid as (
    select
        f.hg_stock_id
        , min(cast(v.published_date as date)) as first_published_date
        , max(cast(v.published_date as date)) as last_published_date
    from {{ ref('fact_editing') }} f
    join {{ ref('bridge_bt_vid') }} b
        on f.editing_code = b.editing_code
    join {{ ref('dim_video') }} v
        on b.video_id = v.video_id
    where v.published_date is not null
    group by f.hg_stock_id
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
    where h."ToStatus" = 'Archived'
        and rf."MediaType" = 1
        and nullif(trim(cast(rf."Id" as text)), '') is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['s.hg_stock_id']) }} as dim_stock_sk
    , s.hg_stock_id
    , nullif(trim(cast(s.name as text)), '') as name
    , s.isrc
    , s.stock_link
    , cast(s.created as timestamp) as stock_stored_date
    , v.first_published_date
    , current_date - cast(s.created as date) as resource_age_days
    , case
        when a.hg_stock_id is not null then 'Lưu kho'
        when v.hg_stock_id is null then 'Tồn kho'
        when v.last_published_date < current_date - 30 then 'Hàng nguội'
        else 'Sử dụng'
      end as status
from dedup_by_stock s
left join vid v
    on s.hg_stock_id = v.hg_stock_id
left join archived a
    on s.hg_stock_id = a.hg_stock_id