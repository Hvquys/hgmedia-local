-- silver.fact_editing
select
    {{ dbt_utils.generate_surrogate_key(['re."Id"']) }} as fact_editing_sk
    , e."Id" as editing_id
    , e."EditingFileId" as editing_code
    , r."ResourceFileId" as hg_stock_id
    , row_number() over (
        partition by re."EditingId"
        order by re."StartTime", re."Id"
    ) as position
    , cast(re."EndTime" as numeric) - cast(re."StartTime" as numeric) as duration
from {{ source('staging', 'resource_editings') }} re
join {{ source('staging', 'editings') }} e on re."EditingId" = e."Id"
join {{ source('staging', 'resource') }} r on re."ResourcesId" = r."Id"
where r."ResourceType" = 0 and r."ResourceFileId" like 'HGFA%'
