with channel_user_map as (

    select distinct on (cu."ChannelId")
        cu."ChannelId" as channel_id,
        u."FullName"   as employee_name
    from {{ source('staging', 'channel_user') }} cu
    left join {{ source('staging', 'user') }} u on cu."UserId" = u."Id"
    where cu."IsDeleted" = false
    order by cu."ChannelId", cu."CreatedTimeUtc" desc

),

channel_network_map as (

    select
        cd."ChannelId"  as channel_id,
        n."Id"          as network_id
    from {{ source('staging', 'channel_deal') }} cd
    left join {{ source('staging', 'cms') }} cms on cd."CmsId" = cms."Id"
    left join {{ source('staging', 'network') }} n on cms."NetworkId" = n."Id"
    where cd."IsDeleted" = false
      and cd."IsOutNet" = false

),

channel_company_map as (

    select distinct on (cd."ChannelId")
        cd."ChannelId" as channel_id,
        d."Id"         as company_id
    from {{ source('staging', 'channeldepartment') }} cd
    inner join {{ source('staging', 'department') }} d
        on cd."DepartmentId" = d."Id"
    inner join {{ source('staging', 'departmentlevel') }} dl
        on d."DepartmentLevelId" = dl."Id"
    where dl."Name" like '%Công ty%'
    order by cd."ChannelId"

)

select distinct on (c."YoutubeChannelId")
    {{ dbt_utils.generate_surrogate_key(['c."YoutubeChannelId"']) }} as dim_channel_sk
    , nullif(trim(cast(c."YoutubeChannelId" as text)), '') as channel_id
    , nullif(trim(cast(c."Title" as text)), '') as channel_name
    , nullif(trim(cast(ccm.company_id as text)), '') as company_id
    , nullif(trim(cast(cum.employee_name as text)), '') as employee_name
    , nullif(trim(cast(cp."ProjectId" as text)), '') as project_id
    , nullif(trim(cast(p."ParentId" as text)), '') as sub_project_id
    , case
        when nullif(trim(cast(c."YoutubeChannelId" as text)), '') is not null
            then 'https://www.youtube.com/channel/' || trim(cast(c."YoutubeChannelId" as text))
        else null
      end as link
    , nullif(trim(cast(cnm.network_id as text)), '') as network_id
from {{ source('staging', 'channel') }} c
left join {{ source('staging', 'channel_project') }} cp on c."Id" = cp."ChannelId"
left join {{ source('staging', 'project') }} p on cp."ProjectId" = p."Id"
left join channel_user_map cum on c."Id" = cum.channel_id
left join channel_network_map cnm on c."Id" = cnm.channel_id
left join channel_company_map ccm on c."Id" = ccm.channel_id
order by c."YoutubeChannelId"