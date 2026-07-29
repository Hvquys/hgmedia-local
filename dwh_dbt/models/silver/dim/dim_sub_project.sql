-- select
--     {{ dbt_utils.generate_surrogate_key(['id']) }} as dim_sub_project_sk
--     , nullif(trim(cast(id as text)),'') as sub_project_id
--     , nullif(trim(cast(project_id as text)),'') as project_id
--     , nullif(trim(cast(name as text)),'') as sub_project_name
--     , nullif(trim(cast(description as text)),'') as description
-- from {{ source('staging', 'x_product_genre') }}



with from_odoo as (
    select
        nullif(trim(cast(id as text)),'') as sub_project_id
        , nullif(trim(cast(project_id as text)),'') as project_id
        , coalesce(nullif(trim(cast(name as text)),''), 'Không có dự án con') as sub_project_name
        , nullif(trim(cast(description as text)),'') as description
    from {{ source('staging', 'x_product_genre') }}
    where order_type = 'music'
),

from_resource_before_odoo as (
    select
        {{ dbt_utils.generate_surrogate_key(['"Subgenre"']) }} as sub_project_id
        , coalesce(
            (
                select dp.project_id
                from {{ ref('dim_project') }} dp
                where dp.project_name = nullif(trim(rbo."Thể loại"),'')
                limit 1
            ),
            (
                select nullif(trim(cast(xpg.project_id as text)),'')
                from {{ source('staging', 'x_product_genre') }} xpg
                where nullif(trim(cast(xpg.name as text)),'') = nullif(trim(rbo."Thể loại"),'')
                limit 1
            ),
            {{ dbt_utils.generate_surrogate_key(['rbo."Thể loại"']) }}
        ) as project_id
        , coalesce(nullif(trim(rbo."Subgenre"),''), 'Không có dự án con') as sub_project_name
        , null as description
    from {{ source('staging', 'resource_before_odoo') }} rbo
    where nullif(trim(rbo."Subgenre"),'') is not null
        and nullif(trim(rbo."Subgenre"),'') not in (
            select sub_project_name
            from from_odoo
            where sub_project_name is not null
        )
),

combined as (
    select * from from_odoo
    union all
    select * from from_resource_before_odoo
),

deduped as (
    select distinct on (sub_project_name)
        sub_project_id
        , project_id
        , sub_project_name
        , description
    from combined
    order by sub_project_name
),

-- sinh n dòng 'Không có dự án con' ứng với n project_id
default_per_project as (
    select
        {{ dbt_utils.generate_surrogate_key(['dp.project_id', "'Không có dự án con'"]) }} as sub_project_id
        , dp.project_id
        , 'Không có dự án con' as sub_project_name
        , null as description
    from {{ ref('dim_project') }} dp
    where not exists (
        select 1 from from_odoo fo
        where fo.project_id = dp.project_id
            and fo.sub_project_name = 'Không có dự án con'
    )
),

with_default as (
    select * from deduped
    union all
    select * from default_per_project
)

select
    {{ dbt_utils.generate_surrogate_key(['sub_project_id']) }} as dim_sub_project_sk
    , sub_project_id
    , project_id
    , sub_project_name
    , description
from with_default