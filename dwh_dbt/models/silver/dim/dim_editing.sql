select
    {{ dbt_utils.generate_surrogate_key(['"EditingFileId"']) }} as dim_editing_sk
    , nullif(trim("EditingFileId"),'') as editing_code
    , nullif(trim("Title"),'') as editing_name
    , nullif(cast("EditingSoftware" as text),'') as software
    , cast(nullif(trim(cast("CreatedDate" as text)),'') as timestamp) as date
from {{ source('staging','editings') }}
where nullif(trim("EditingFileId"),'') is not null
group by "EditingFileId", "Title", "EditingSoftware", "CreatedDate"