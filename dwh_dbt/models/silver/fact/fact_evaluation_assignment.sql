select
    {{ dbt_utils.generate_surrogate_key(['r."Id"']) }} as music_evaluation_id
    , nullif(trim(cast(r."CreatedByUserId" as text)),'') as reviewer_id
    , r."Rating"::numeric as score
    , nullif(trim(u."FullName"),'') as reviewer_name
from {{ source('staging','review') }} r
left join {{ source('staging','review_user') }} u
    on cast(r."CreatedByUserId" as text) = cast(u."Id" as text)