-- silver.dim_video  (target theo Data Dictionary)
with source_data as (
    select
        *
        , row_number() over (
            partition by "YoutubeVideoId"
            order by "PublishedAt" desc
        ) as rn
    from {{ source('staging','channel_video_info') }}
    where nullif(trim("YoutubeVideoId"),'') is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['"YoutubeVideoId"']) }} as dim_video_sk
    , nullif(trim("YoutubeVideoId"),'') as video_id
    , nullif(trim("YoutubeChannelId"),'') as channel_id
    , 'https://www.youtube.com/watch?v=' || "YoutubeVideoId" as video_url
    , nullif(trim("Code"),'') as editing_code
    , cast("PublishedAt" as timestamp) as published_date
    , nullif(trim("Title"),'') as video_name
from source_data
where rn = 1