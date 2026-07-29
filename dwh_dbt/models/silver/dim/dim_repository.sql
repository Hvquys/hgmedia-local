with from_odoo as (
    select
        nullif(trim(cast(id as text)),'') as repository_id
        , nullif(trim(cast(product_genre_id as text)),'') as sub_project_id
        , nullif(trim(cast(name as text)),'') as repository_name
    from {{ source('staging', 'x_product_subgenre') }}
    where nullif(trim(cast(product_genre_id as text)),'') is not null
        and order_type = 'music'   -- chỉ lấy bản ghi thuộc order_type music
),

from_partners as (
    select
        {{ dbt_utils.generate_surrogate_key(['p."Tên đối tác"']) }} as repository_id
        , dsp.sub_project_id
        , nullif(trim(p."Tên đối tác"),'') as repository_name
    from {{ source('staging', 'partners') }} p
    left join {{ ref('dim_project') }} dp
        on nullif(trim(p."Dự án"),'') = dp.project_name
    left join {{ ref('dim_sub_project') }} dsp
        on dp.project_id = dsp.project_id
        and dsp.sub_project_name = 'Không có dự án con'
    where nullif(trim(p."Tên đối tác"),'') is not null
        and nullif(trim(p."Tên đối tác"),'') not in (
            select repository_name
            from from_odoo
            where repository_name is not null
        )
),

combined as (
    select * from from_odoo

    union all

    select * from from_partners
),

deduped as (
    select distinct on (repository_name, sub_project_id)
        repository_id
        , sub_project_id
        , repository_name
    from combined
    order by repository_name, sub_project_id
),

-- Sinh thêm "Không có kho" cho tất cả dự án con
default_per_sub_project as (
    select
        {{ dbt_utils.generate_surrogate_key(['dsp.sub_project_id', "'Không có kho'"]) }} as repository_id
        , dsp.sub_project_id
        , 'Không có kho' as repository_name
    from {{ ref('dim_sub_project') }} dsp
    where not exists (
        select 1
        from from_odoo fo
        where fo.sub_project_id = dsp.sub_project_id
            and fo.repository_name = 'Không có kho'
    )
),

with_default as (
    select * from deduped

    union all

    select * from default_per_sub_project
)

select
    {{ dbt_utils.generate_surrogate_key(['repository_id']) }} as dim_repository_sk
    , repository_id
    , sub_project_id
    , repository_name
from with_default