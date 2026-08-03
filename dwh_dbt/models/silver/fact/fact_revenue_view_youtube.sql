{{ config(materialized='table') }}

with video_dim as (
    select
        nullif(trim("YoutubeVideoId"), '') as video_id,
        nullif(trim("YoutubeChannelId"), '') as channel_id,
        'https://www.youtube.com/watch?v=' || "YoutubeVideoId" as video_url,
        nullif(trim("Code"), '') as editing_code,
        cast("PublishedAt" as timestamp) as published_date,
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
        channel_id,
        video_url,
        editing_code,
        published_date,
        video_name
    from video_dim
    where rn = 1
),

bg_media_channels as (
    select distinct
        nullif(trim(c."YoutubeChannelId"), '') as channel_id
    from {{ source('staging', 'channel_company') }} cc
    join {{ source('staging', 'channel') }} c
        on cc."ChannelId" = c."Id"
    where cc."CompanyId" = '36310203-8005-41dc-b51e-a8dc52960496'
        and nullif(trim(c."YoutubeChannelId"), '') is not null
),

video_metric_source as (
    select
        nullif(trim(m."YoutubeVideoId"), '') as video_id,
        m."Date"::date as record_date,
        coalesce(m."EstimatedRevenue"::numeric, 0) as revenue_amount,
        coalesce(m."Views"::numeric, 0) as view
    from {{ source('staging', 'channel_video_metric') }} m
    left join bg_media_channels bg
        on nullif(trim(m."YoutubeChannelId"), '') = bg.channel_id
    where nullif(trim(m."YoutubeVideoId"), '') is not null
        and m."Date" is not null
        and bg.channel_id is null
),

usd as (
    select distinct on (record_date)
        record_date,
        exchange_rate
    from {{ ref('dim_usd') }}
    order by record_date
)

select
    {{ dbt_utils.generate_surrogate_key([
        'm.video_id',
        'm.record_date'
    ]) }} as fact_revenue_view_youtube_sk,

    m.video_id,
    dv.channel_id,
    dv.video_url,
    dv.editing_code,
    dv.published_date,
    dv.video_name,

    m.revenue_amount,
    m.view,

    case
        when m.view = 0 then 0
        else m.revenue_amount / m.view * 1000
    end as rpm,

    m.record_date::timestamp as revenue_date,
    u.exchange_rate

from video_metric_source m
left join dim_video dv
    on m.video_id = dv.video_id
left join usd u
    on m.record_date = u.record_date
