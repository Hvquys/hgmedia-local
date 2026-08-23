{{ config(materialized='table') }}

with video_dim as (
    select
        nullif(trim("YoutubeVideoId"), '') as video_id,
        'https://www.youtube.com/watch?v=' || "YoutubeVideoId" as video_url,
        ("PublishedAt" at time zone 'UTC') as published_date,
        nullif(trim("Title"), '') as video_name,
        row_number() over (
            partition by "YoutubeVideoId"
            order by "PublishedAt" desc
        ) as rn
    from {{ source('staging', 'channel_video_info') }}
    where nullif(trim("YoutubeVideoId"), '') is not null
),

dim_video as (
    select
        video_id,
        video_url,
        published_date,
        video_name
    from video_dim
    where rn = 1
),

channel_master as (
    select
        ch."Id" as channel_pk,
        nullif(trim(ch."YoutubeChannelId"), '') as channel_id
    from {{ source('staging', 'channel') }} ch
    where nullif(trim(ch."YoutubeChannelId"), '') is not null
      and coalesce(ch."IsDeleted", false) = false
      and exists (
          select 1
          from {{ source('staging', 'channel_company') }} cc
          where cc."ChannelId" = ch."Id"
            and coalesce(cc."IsDeleted", false) = false
            and cc."CompanyId" = '{{ var("target_company_id", "448025a0-0ff7-4ed0-a917-399a73decef2") }}'
      )
),

video_metric_source as (
    select
        nullif(trim(m."YoutubeVideoId"), '') as video_id,
        nullif(trim(m."YoutubeChannelId"), '') as channel_id,
        (m."Date" at time zone 'UTC')::date as revenue_date,
        m."EstimatedRevenue"::numeric as revenue_amount_raw,
        m."Views"::numeric as view_raw
    from {{ source('staging', 'channel_video_metric') }} m
    join channel_master cm
        on nullif(trim(m."YoutubeChannelId"), '') = cm.channel_id
    where nullif(trim(m."YoutubeVideoId"), '') is not null
      and m."Date" is not null
)

select
    {{ dbt_utils.generate_surrogate_key([
        'm.channel_id',
        'm.video_id',
        'm.revenue_date'
    ]) }} as fact_revenue_view_youtube_sk,
    m.video_id,
    m.channel_id,
    dv.video_url,
    dv.published_date,
    dv.video_name,
    coalesce(m.revenue_amount_raw, 0) as revenue_amount,
    coalesce(m.view_raw, 0) as view,
    case
        when coalesce(m.view_raw, 0) = 0 then 0
        else coalesce(m.revenue_amount_raw, 0) / m.view_raw * 1000
    end as rpm,
    m.revenue_date
from video_metric_source m
left join dim_video dv
    on m.video_id = dv.video_id