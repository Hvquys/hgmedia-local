-- select
--     {{ dbt_utils.generate_surrogate_key(['id']) }} as dim_project_sk
--     , nullif(trim(cast(id as text)),'') as project_id
--     , nullif(trim(cast(name as text)),'') as project_name
--     , nullif(trim(cast(state as text)),'') as status
-- from {{ source('staging', 'x_project') }}

with from_odoo as (
    select
        nullif(trim(cast(id as text)), '') as project_id
        , nullif(trim(cast(name as text)), '') as project_name
        , nullif(trim(cast(state as text)), '') as status
    from {{ source('staging', 'x_project') }}
),

from_partners as (
    select
        {{ dbt_utils.generate_surrogate_key(['"Dự án"']) }} as project_id
        , nullif(trim("Dự án"), '') as project_name
        , cast(null as text) as status
    from {{ source('staging', 'partners') }}
    where nullif(trim("Dự án"), '') is not null
        and nullif(trim("Dự án"), '') not in (
            select project_name
            from from_odoo
            where project_name is not null
        )
),

base_projects as (
    select * from from_odoo
    union all
    select * from from_partners
),

missing_project_from_sub_project as (
    select distinct on (dsp_project_id)
        dsp_project_id as project_id
        , project_name
        , cast(null as text) as status
    from (
        select
            nullif(trim(cast(dsp.project_id as text)), '') as dsp_project_id
            , nullif(trim(rbo."Thể loại"), '') as project_name
        from silver.dim_sub_project dsp
        join {{ source('staging', 'resource_before_odoo') }} rbo
            on lower(regexp_replace(normalize(nullif(trim(dsp.sub_project_name), ''), nfc), '\s+', ' ', 'g'))
                = lower(regexp_replace(normalize(nullif(trim(rbo."Subgenre"), ''), nfc), '\s+', ' ', 'g'))
        left join base_projects bp
            on nullif(trim(cast(dsp.project_id as text)), '') = bp.project_id
        where nullif(trim(cast(dsp.project_id as text)), '') is not null
            and bp.project_id is null
            and nullif(trim(rbo."Thể loại"), '') is not null
    ) missing
    order by dsp_project_id, project_name
),

combined as (
    select * from base_projects
    union all
    select * from missing_project_from_sub_project
)

select distinct on (project_id)
    {{ dbt_utils.generate_surrogate_key(['project_id']) }} as dim_project_sk
    , project_id
    , project_name
    , status
from combined
where project_id is not null
order by project_id, project_name, status
