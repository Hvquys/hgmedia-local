{{ config(materialized='table') }}

with channel_master as (
    select distinct
        nullif(trim(ch."YoutubeChannelId"), '') as channel_id
    from {{ source('staging', 'channel') }} ch
    where nullif(trim(ch."YoutubeChannelId"), '') is not null
      and exists (
          select 1
          from {{ source('staging', 'channel_company') }} cc
          where cc."ChannelId" = ch."Id"
            and coalesce(cc."IsDeleted", false) = false
            and cc."CompanyId" = '{{ var("target_company_id", "448025a0-0ff7-4ed0-a917-399a73decef2") }}'
      )
),

vid_metric as (
    select
        nullif(trim(m."YoutubeVideoId"), '') as video_id,
        m."Date"::date as recorded_date,
        sum(m."EstimatedRevenue"::numeric) as revenue,
        sum(m."Views"::numeric) as views
    from {{ source('staging', 'channel_video_metric') }} m
    join channel_master cm
        on nullif(trim(m."YoutubeChannelId"), '') = cm.channel_id
    where nullif(trim(m."YoutubeVideoId"), '') is not null
      and m."Date" is not null
    group by
        nullif(trim(m."YoutubeVideoId"), ''),
        m."Date"::date
),

bridge_video_editing as (
    select distinct
        nullif(trim(video_id), '') as video_id,
        nullif(trim(editing_code), '') as editing_code
    from {{ ref('bridge_bt_vid') }}
    where nullif(trim(video_id), '') is not null
      and nullif(trim(editing_code), '') is not null
),

editing_resources as (
    select distinct
        editing_code,
        hg_stock_id,
        position
    from {{ ref('fact_editing') }}
    where editing_code is not null
      and hg_stock_id is not null
      and position between 1 and 5
),

vid_res as (
    select
        b.video_id,
        b.editing_code,
        f.hg_stock_id,
        f.position,
        count(*) over (
            partition by b.video_id, b.editing_code
        ) as n_res
    from bridge_video_editing b
    join editing_resources f
        on b.editing_code = f.editing_code
),

weighted as (
    select
        vr.video_id,
        vr.editing_code,
        vr.hg_stock_id,
        vr.position,
        vr.n_res,
        case
            when vr.n_res = 1 then 10

            when vr.n_res = 2
                and vr.position in (1, 2) then 5

            when vr.n_res = 3
                and vr.position = 1 then 6
            when vr.n_res = 3
                and vr.position = 2 then 3
            when vr.n_res = 3
                and vr.position = 3 then 1

            when vr.n_res = 4
                and vr.position = 1 then 6
            when vr.n_res = 4
                and vr.position = 2 then 2
            when vr.n_res = 4
                and vr.position in (3, 4) then 1

            when vr.n_res >= 5
                and vr.position = 1 then 5
            when vr.n_res >= 5
                and vr.position = 2 then 2
            when vr.n_res >= 5
                and vr.position in (3, 4, 5) then 1

            else 0
        end as w
    from vid_res vr
),

video_dim as (
    select distinct on (nullif(trim(video_id), ''))
        nullif(trim(video_id), '') as video_id,
        video_name,
        video_url
    from {{ ref('dim_video') }}
    where nullif(trim(video_id), '') is not null
    order by
        nullif(trim(video_id), ''),
        published_date desc nulls last
)

select
    {{ dbt_utils.generate_surrogate_key([
        'w.video_id',
        'w.editing_code',
        'w.hg_stock_id',
        'w.position::text',
        'm.recorded_date::text'
    ]) }} as revenue_id,

    w.video_id,
    dv.video_name,
    dv.video_url,
    w.editing_code,
    w.hg_stock_id as resource_id,
    w.position,
    m.revenue * w.w / 10.0 as revenue_amount,
    m.recorded_date::timestamp as recorded_date,
    m.views * w.w / 10.0 as view

from weighted w
join vid_metric m
    on w.video_id = m.video_id
left join video_dim dv
    on w.video_id = dv.video_id
where w.w > 0