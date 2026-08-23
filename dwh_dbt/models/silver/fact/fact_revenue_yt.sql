{{ config(materialized='table') }}

with net_channels as (
    select distinct c."YoutubeChannelId"
    from {{ source('staging','channel_deal') }} cd
    join {{ source('staging','channel_company') }} cc on cd."ChannelId" = cc."ChannelId"
    join {{ source('staging','channel') }} c on cd."ChannelId" = c."Id"
    where cc."Type" is null
        and cd."IsOutNet" = false
        and cd."IsDeleted" = false
        and cd."ReceiveTimeUtc" is not null
        and c."SuspendedTimeUtc" is null
),

usd as (
    select distinct on (record_date)
        record_date
        , exchange_rate
    from {{ ref('dim_usd') }}
    order by record_date
)

select
    {{ dbt_utils.generate_surrogate_key(['m."YoutubeVideoId"', 'm."Date"']) }} as fact_revenue_yt_sk
    , m."YoutubeVideoId" as video_id
    , m."EstimatedRevenue"::numeric as revenue_amount
    , m."Date"::timestamp as revenue_date
    , u.exchange_rate
from {{ source('staging','channel_video_metric') }} m
join {{ source('staging','channel_video_info') }} cv on m."YoutubeVideoId" = cv."YoutubeVideoId"
join net_channels nc on cv."YoutubeChannelId" = nc."YoutubeChannelId"
left join usd u on m."Date"::date = u.record_date
where m."YoutubeVideoId" is not null
    and trim(m."YoutubeVideoId") <> ''
    and m."Date" is not null