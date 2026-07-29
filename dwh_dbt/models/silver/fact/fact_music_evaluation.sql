with ver as (
    select
        "Id",
        "RecordingSoundId",
        "CreatedDate",
        row_number() over (
            partition by "RecordingSoundId"
            order by "CreatedDate", "Id"
        ) as version_no
    from {{ source('staging','recording_sound_version') }}
),

mkt as (
    select
        vnc."RecordingSoundVersionId" as version_id,
        avg(ucv."Rating"::numeric) as marketing_score
    from {{ source('staging','user_comment_version') }} ucv
    join {{ source('staging','version_needs_comment') }} vnc
        on cast(ucv."VersionNeedsCommentId" as text) = cast(vnc."Id" as text)
    where cast(ucv."UserId" as text) <> '13c172f2-f476-414f-afcd-2f6635ed4ace'
    group by vnc."RecordingSoundVersionId"
)

select
    {{ dbt_utils.generate_surrogate_key(['r."Id"']) }} as music_evaluation_id,
    rs."SongCode" as resource_id,
    v.version_no as version,

    case
        when cast(r."CreatedByUserId" as text) = '13c172f2-f476-414f-afcd-2f6635ed4ace'
        then r."Rating"::numeric
    end as internal_score,

    mkt.marketing_score,

    r."Concluding" as status,

    case r."Concluding"
        when 0 then 'Chưa kết luận'
        when 1 then 'Đạt'
        when 2 then 'Không đạt'
        when 3 then 'Cần sửa'
    end as status_vi,

    case
        when left(r."Comment", 1) = '{'
        then r."Comment"::json->>'Comment'
        else r."Comment"
    end as comment,

    cast(r."CreatedDate" as timestamp) as review_date,

    cast(rs."CreatedDate" as timestamp) as uploaded_date

from {{ source('staging','review') }} r
left join ver v
    on cast(r."RecordingSoundVersionId" as text) = cast(v."Id" as text)
left join {{ source('staging','recording_sound') }} rs
    on cast(v."RecordingSoundId" as text) = cast(rs."Id" as text)
left join mkt
    on cast(r."RecordingSoundVersionId" as text) = cast(mkt.version_id as text)
