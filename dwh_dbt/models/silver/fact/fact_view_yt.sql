with net_channels as (
    select distinct c."YoutubeChannelId"
    from {{ source('staging','channel_deal') }} cd
    join {{ source('staging','channel_company') }} cc on cd."ChannelId" = cc."ChannelId"
    join {{ source('staging','channel') }} c on cd."ChannelId" = c."Id"
    where cc."Type" is null
        and cd."IsOutNet" = false
        and cd."IsDeleted" = false
        and cd."OutNetTimeUtc" is null
        and c."SuspendedTimeUtc" is null
)

select
    {{ dbt_utils.generate_surrogate_key(['m."YoutubeVideoId"', 'm."Date"']) }} as fact_view_yt_sk
    , m."YoutubeVideoId" as video_id
    , m."Views"::numeric as view_count
    , m."Date"::timestamp as recorded_date
    , m."EstimatedRevenue"::numeric / nullif(m."Views"::numeric, 0) as rpm
from {{ source('staging','channel_video_metric') }} m
join {{ source('staging','channel_video_info') }} cv on m."YoutubeVideoId" = cv."YoutubeVideoId"
join net_channels nc on cv."YoutubeChannelId" = nc."YoutubeChannelId"
where m."YoutubeVideoId" is not null
    and trim(m."YoutubeVideoId") <> ''
    and m."Date" is not null