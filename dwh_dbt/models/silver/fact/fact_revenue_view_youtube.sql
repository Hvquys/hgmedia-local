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
    from {{ source('staging','channel_video_info') }}
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

net_channels as (
    select distinct
        c."YoutubeChannelId" as channel_id
    from {{ source('staging','channel_deal') }} cd
    join {{ source('staging','channel_company') }} cc
        on cd."ChannelId" = cc."ChannelId"
    join {{ source('staging','channel') }} c
        on cd."ChannelId" = c."Id"
    where cc."Type" is null
        and cd."IsOutNet" = false
        and cd."IsDeleted" = false
        and cd."ReceiveTimeUtc" is not null
        and c."SuspendedTimeUtc" is null
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
        'm."YoutubeVideoId"',
        'm."Date"'
    ]) }} as fact_revenue_view_youtube_sk,

    m."YoutubeVideoId" as video_id,
    dv.channel_id,
    dv.video_url,
    dv.editing_code,
    dv.published_date,
    dv.video_name,

    m."EstimatedRevenue"::numeric as revenue_amount,
    m."Views"::numeric as view,

    case
        when coalesce(m."Views"::numeric, 0) = 0 then 0
        else m."EstimatedRevenue"::numeric / m."Views"::numeric * 1000
    end as rpm,

    m."Date"::timestamp as revenue_date,
    u.exchange_rate

from {{ source('staging','channel_video_metric') }} m
join dim_video dv
    on m."YoutubeVideoId" = dv.video_id
join net_channels nc
    on dv.channel_id = nc.channel_id
left join usd u
    on m."Date"::date = u.record_date
where m."YoutubeVideoId" is not null
    and trim(m."YoutubeVideoId") <> ''
    and m."Date" is not null
