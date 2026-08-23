-- silver.bridge_bt_vid
select
    {{ dbt_utils.generate_surrogate_key(['"YoutubeVideoId"', 'editing_code']) }} as bridge_bt_vid_sk
    , editing_code
    , nullif(trim("YoutubeVideoId"),'') as video_id
from (
    select
        "YoutubeVideoId",
        nullif(trim("Code"),'') as editing_code
    from {{ source('staging','channel_video_info') }}
) sub
where nullif(trim(editing_code),'') is not null