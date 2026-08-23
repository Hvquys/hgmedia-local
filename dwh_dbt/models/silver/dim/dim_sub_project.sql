-- Replacement model for dim_sub_project.
-- project_id_overrides is retained for the existing 129 -> 163 business mapping.
-- Distro values are matched only by NFC + trim + collapsed whitespace + case.
-- Distinct suffixes and spelling differences remain distinct sub-projects.

with project_id_overrides as (
    select '129'::text as legacy_project_id, '163'::text as canonical_project_id
),

canonical_project_aliases as (
    select distinct on (source_project_id)
        source_project_id
        , canonical_project_id
    from (
        -- Preserve every Odoo ID as an alias, but point it at the
        -- canonical ID selected by dim_project for the same normalised name.
        select
            nullif(trim(cast(xp.id as text)), '') as source_project_id
            , coalesce(o.canonical_project_id, dp.project_id) as canonical_project_id
        from {{ source('staging', 'x_project') }} xp
        inner join {{ ref('dim_project') }} dp
            on lower(regexp_replace(normalize(trim(xp.name), nfc), '\s+', ' ', 'g'))
                = lower(regexp_replace(normalize(trim(dp.project_name), nfc), '\s+', ' ', 'g'))
        left join project_id_overrides o
            on nullif(trim(cast(xp.id as text)), '') = o.legacy_project_id

        union all

        select
            dp.project_id as source_project_id
            , coalesce(o.canonical_project_id, dp.project_id) as canonical_project_id
        from {{ ref('dim_project') }} dp
        left join project_id_overrides o
            on dp.project_id = o.legacy_project_id
    ) aliases
    where source_project_id is not null
        and canonical_project_id is not null
    order by
        source_project_id
        , case when canonical_project_id ~ '^[0-9]+$' then 0 else 1 end
        , case when canonical_project_id ~ '^[0-9]+$'
            then canonical_project_id::numeric end nulls last
        , canonical_project_id
),

from_odoo as (
    select
        nullif(trim(cast(xpg.id as text)), '') as sub_project_id
        , coalesce(
            o.canonical_project_id,
            pa.canonical_project_id,
            nullif(trim(cast(xpg.project_id as text)), ''),
            (
                select nullif(trim(cast(xp.id as text)), '')
                from {{ source('staging', 'x_project') }} xp
                where lower(normalize(trim(xp.name), nfc))
                    = lower(normalize(trim(xpg.name), nfc))
                order by xp.id
                limit 1
            )
        ) as project_id
        , coalesce(
            nullif(regexp_replace(normalize(trim(cast(xpg.name as text)), nfc), '\s+', ' ', 'g'), ''),
            'Không có dự án con'
        ) as sub_project_name
        , nullif(trim(cast(xpg.description as text)), '') as description
        , 1 as source_priority
    from {{ source('staging', 'x_product_genre') }} xpg
    left join project_id_overrides o
        on nullif(trim(cast(xpg.project_id as text)), '') = o.legacy_project_id
    left join canonical_project_aliases pa
        on nullif(trim(cast(xpg.project_id as text)), '') = pa.source_project_id
    where xpg.order_type = 'music'
),

resource_before_odoo_resolved_raw as (
    select
        coalesce(
            (
                select dp.project_id
                from {{ ref('dim_project') }} dp
                where lower(normalize(trim(dp.project_name), nfc))
                    = lower(normalize(trim(rbo."Thể loại"), nfc))
                order by
                    case when dp.project_id ~ '^[0-9]+$' then 0 else 1 end,
                    case when dp.project_id ~ '^[0-9]+$' then dp.project_id::numeric end nulls last,
                    dp.project_id
                limit 1
            ),
            (
                select pa.canonical_project_id
                from {{ source('staging', 'x_project') }} xp
                inner join canonical_project_aliases pa
                    on pa.source_project_id = nullif(trim(cast(xp.id as text)), '')
                where lower(regexp_replace(normalize(trim(xp.name), nfc), '\s+', ' ', 'g'))
                    = lower(regexp_replace(normalize(trim(rbo."Thể loại"), nfc), '\s+', ' ', 'g'))
                order by
                    case when pa.canonical_project_id ~ '^[0-9]+$' then 0 else 1 end,
                    case when pa.canonical_project_id ~ '^[0-9]+$'
                        then pa.canonical_project_id::numeric end nulls last,
                    pa.canonical_project_id
                limit 1
            ),
            -- Must use the same canonical key as dim_project_rbo_complete.
            {{ dbt_utils.generate_surrogate_key([
                "lower(regexp_replace(normalize(trim(rbo.\"Thể loại\"), nfc), '\\s+', ' ', 'g'))"
            ]) }}
        ) as project_id
        , coalesce(
            nullif(regexp_replace(normalize(trim(rbo."Subgenre"), nfc), '\s+', ' ', 'g'), ''),
            'Không có dự án con'
        ) as sub_project_name
    from {{ source('staging', 'resource_before_odoo') }} rbo
    where nullif(trim(rbo."Subgenre"), '') is not null
),

resource_before_odoo_resolved as (
    select
        coalesce(o.canonical_project_id, r.project_id) as project_id
        , r.sub_project_name
    from resource_before_odoo_resolved_raw r
    left join project_id_overrides o on r.project_id = o.legacy_project_id
),

from_resource_before_odoo as (
    select
        {{ dbt_utils.generate_surrogate_key(['project_id', 'sub_project_name']) }} as sub_project_id
        , project_id
        , sub_project_name
        , cast(null as text) as description
        , 2 as source_priority
    from resource_before_odoo_resolved
    where project_id is not null
),

performance_clean as (
    select distinct
        nullif(regexp_replace(normalize(trim(p."Dự án chốt"), nfc), '\s+', ' ', 'g'), '') as project_name
        , case
            when nullif(trim(p."Dự án con (nếu có)"), '') is null
                or upper(trim(p."Dự án con (nếu có)")) = '#N/A'
                or lower(normalize(trim(p."Dự án con (nếu có)"), nfc)) = 'không có'
                then 'Không có dự án con'
            else nullif(regexp_replace(normalize(trim(p."Dự án con (nếu có)"), nfc), '\s+', ' ', 'g'), '')
          end as sub_project_name
    from {{ source('staging', 'resource_performance') }} p
    where nullif(trim(p."Dự án chốt"), '') is not null
        and upper(trim(p."Dự án chốt")) <> '#N/A'
        and lower(normalize(trim(p."Dự án chốt"), nfc)) <> 'không xác định'
),

performance_resolved as (
    select distinct
        coalesce(o.canonical_project_id, dp.project_id) as project_id
        , pc.sub_project_name
    from performance_clean pc
    inner join {{ ref('dim_project') }} dp
        on lower(normalize(pc.project_name, nfc))
            = lower(normalize(dp.project_name, nfc))
    left join project_id_overrides o on dp.project_id = o.legacy_project_id
),

from_performance as (
    select
        {{ dbt_utils.generate_surrogate_key(['pr.project_id', 'pr.sub_project_name']) }} as sub_project_id
        , pr.project_id
        , pr.sub_project_name
        , cast(null as text) as description
        , 3 as source_priority
    from performance_resolved pr
    where not exists (
        select 1
        from from_odoo fo
        where fo.project_id = pr.project_id
            and lower(normalize(fo.sub_project_name, nfc))
                = lower(normalize(pr.sub_project_name, nfc))
    )
),

combined_base as (
    select * from from_odoo
    union all
    select * from from_resource_before_odoo
    union all
    select * from from_performance
),

canonical_combined_base as (
    select
        c.sub_project_id
        , coalesce(o.canonical_project_id, c.project_id) as project_id
        , nullif(regexp_replace(normalize(trim(c.sub_project_name), nfc), '\s+', ' ', 'g'), '') as sub_project_name
        , c.description
        , c.source_priority
    from combined_base c
    left join project_id_overrides o on c.project_id = o.legacy_project_id
    where c.sub_project_id is not null
        and c.project_id is not null
        and nullif(trim(c.sub_project_name), '') is not null
),

deduped_base as (
    select distinct on (
        project_id,
        lower(normalize(sub_project_name, nfc))
    )
        sub_project_id
        , project_id
        , sub_project_name
        , description
    from canonical_combined_base
    order by
        project_id
        , lower(normalize(sub_project_name, nfc))
        , case when sub_project_id ~ '^[0-9]+$' then 0 else 1 end
        , case when sub_project_id ~ '^[0-9]+$' then sub_project_id::numeric end nulls last
        , source_priority
        , sub_project_id
),

-- Do not split Distro's project value on '/'. Those rows provide one combined
-- sub-project value for multiple projects and cannot be assigned safely.
-- A Distro sub-project is resolved only when its project name maps exactly to
-- one canonical dim_project after standard normalisation.
distro_sub_project_values as (
    select
        nullif(regexp_replace(normalize(trim(cast(distro."Dự án" as text)), nfc), '\s+', ' ', 'g'), '') as project_name
        , nullif(regexp_replace(normalize(trim(cast(distro."Dự án con" as text)), nfc), '\s+', ' ', 'g'), '') as sub_project_name
    from {{ source('staging', 'distro_infomation') }} distro
    where nullif(trim(cast(distro."Dự án" as text)), '') is not null
        and nullif(trim(cast(distro."Dự án con" as text)), '') is not null
),

-- Approved, parent-scoped aliases only. Do not add generic fuzzy matching.
distro_sub_project_name_aliases as (
    select *
    from (
        values
            ('classical', 'classcial bật cid', 'classical bật cid')
    ) as aliases(
        project_name_key,
        source_sub_project_name_key,
        target_sub_project_name_key
    )
),

distro_sub_project_names as (
    select distinct on (project_id, sub_project_name_key)
        project_id
        , sub_project_name
        , sub_project_name_key
    from (
        select
            coalesce(o.canonical_project_id, dp.project_id) as project_id
            , distro.sub_project_name
            , coalesce(
                aliases.target_sub_project_name_key,
                lower(distro.sub_project_name)
            ) as sub_project_name_key
        from distro_sub_project_values distro
        inner join {{ ref('dim_project') }} dp
            on lower(regexp_replace(normalize(trim(dp.project_name), nfc), '\s+', ' ', 'g'))
                = lower(distro.project_name)
        left join project_id_overrides o
            on dp.project_id = o.legacy_project_id
        left join distro_sub_project_name_aliases aliases
            on lower(distro.project_name) = aliases.project_name_key
            and lower(distro.sub_project_name) = aliases.source_sub_project_name_key
        where distro.project_name is not null
            and distro.sub_project_name is not null
            and lower(distro.project_name) <> 'dự án'
            and lower(distro.sub_project_name) <> 'dự án con'
    ) distro
    where project_id is not null
    order by project_id, sub_project_name_key, sub_project_name
),

from_distro_information as (
    select
        coalesce(
            base.sub_project_id
            , {{ dbt_utils.generate_surrogate_key([
                'distro.project_id',
                'distro.sub_project_name_key'
            ]) }}
        ) as sub_project_id
        , distro.project_id
        , coalesce(base.sub_project_name, distro.sub_project_name) as sub_project_name
        , base.description
    from distro_sub_project_names distro
    left join deduped_base base
        on base.project_id = distro.project_id
        and lower(regexp_replace(normalize(trim(base.sub_project_name), nfc), '\s+', ' ', 'g'))
            = distro.sub_project_name_key
),

-- resource_infomation_add supplements sub-project coverage. Missing or
-- "Không có" values are deliberately left to default_per_project so their
-- canonical generated IDs remain stable.
resource_information_add_clean as (
    select distinct
        nullif(regexp_replace(
            normalize(trim(cast(resource_add."Dự án" as text)), nfc),
            '\s+',
            ' ',
            'g'
        ), '') as project_name
        , case
            when nullif(trim(cast(resource_add."Dự án con" as text)), '') is null
                or upper(trim(cast(resource_add."Dự án con" as text))) = '#N/A'
                or lower(normalize(trim(cast(resource_add."Dự án con" as text)), nfc)) in (
                    'không có',
                    'khong co'
                )
                then null
            else nullif(regexp_replace(
                normalize(trim(cast(resource_add."Dự án con" as text)), nfc),
                '\s+',
                ' ',
                'g'
            ), '')
          end as sub_project_name
    from {{ source('staging', 'resource_infomation_add') }} resource_add
    where nullif(trim(cast(resource_add."Dự án" as text)), '') is not null
        and upper(trim(cast(resource_add."Dự án" as text))) <> '#N/A'
        and lower(normalize(trim(cast(resource_add."Dự án" as text)), nfc)) not in (
            'không xác định',
            'bỏ bài này',
            'dự án'
        )
),

resource_information_add_sub_project_names as (
    select distinct on (project_id, sub_project_name_key)
        project_id
        , sub_project_name
        , sub_project_name_key
    from (
        select
            coalesce(o.canonical_project_id, dp.project_id) as project_id
            , resource_add.sub_project_name
            , lower(normalize(resource_add.sub_project_name, nfc)) as sub_project_name_key
        from resource_information_add_clean resource_add
        inner join {{ ref('dim_project') }} dp
            on lower(normalize(dp.project_name, nfc))
                = lower(normalize(resource_add.project_name, nfc))
        left join project_id_overrides o
            on dp.project_id = o.legacy_project_id
        where resource_add.project_name is not null
            and resource_add.sub_project_name is not null
            and lower(normalize(resource_add.sub_project_name, nfc)) <> 'dự án con'
    ) resource_add
    where project_id is not null
    order by project_id, sub_project_name_key, sub_project_name
),

combined_with_distro as (
    select
        sub_project_id
        , project_id
        , sub_project_name
        , description
    from deduped_base

    union all

    select
        sub_project_id
        , project_id
        , sub_project_name
        , description
    from from_distro_information
),

deduped_with_distro as (
    select distinct on (
        project_id,
        lower(normalize(sub_project_name, nfc))
    )
        sub_project_id
        , project_id
        , sub_project_name
        , description
    from combined_with_distro
    order by
        project_id
        , lower(normalize(sub_project_name, nfc))
        , case when sub_project_id ~ '^[0-9]+$' then 0 else 1 end
        , case when sub_project_id ~ '^[0-9]+$' then sub_project_id::numeric end nulls last
        , sub_project_id
),

from_resource_information_add as (
    select
        coalesce(
            existing.sub_project_id
            , {{ dbt_utils.generate_surrogate_key([
                'resource_add.project_id',
                'resource_add.sub_project_name_key'
            ]) }}
        ) as sub_project_id
        , resource_add.project_id
        , coalesce(existing.sub_project_name, resource_add.sub_project_name) as sub_project_name
        , existing.description as description
    from resource_information_add_sub_project_names resource_add
    left join deduped_with_distro existing
        on existing.project_id = resource_add.project_id
        and lower(regexp_replace(
            normalize(trim(existing.sub_project_name), nfc),
            '\s+',
            ' ',
            'g'
        )) = resource_add.sub_project_name_key
),

combined as (
    select
        sub_project_id
        , project_id
        , sub_project_name
        , description
    from deduped_base

    union all

    select
        sub_project_id
        , project_id
        , sub_project_name
        , description
    from from_distro_information

    union all

    select
        sub_project_id
        , project_id
        , sub_project_name
        , description
    from from_resource_information_add
),

deduped as (
    select distinct on (
        project_id,
        lower(normalize(sub_project_name, nfc))
    )
        sub_project_id
        , project_id
        , sub_project_name
        , description
    from combined
    order by
        project_id
        , lower(normalize(sub_project_name, nfc))
        , case when sub_project_id ~ '^[0-9]+$' then 0 else 1 end
        , case when sub_project_id ~ '^[0-9]+$' then sub_project_id::numeric end nulls last
        , sub_project_id
),

canonical_projects as (
    select distinct coalesce(o.canonical_project_id, dp.project_id) as project_id
    from {{ ref('dim_project') }} dp
    left join project_id_overrides o on dp.project_id = o.legacy_project_id
),

default_per_project as (
    select
        {{ dbt_utils.generate_surrogate_key(['cp.project_id', "'Không có dự án con'"]) }} as sub_project_id
        , cp.project_id
        , 'Không có dự án con' as sub_project_name
        , cast(null as text) as description
    from canonical_projects cp
    where not exists (
        select 1
        from deduped d
        where d.project_id = cp.project_id
            and lower(normalize(d.sub_project_name, nfc))
                = lower(normalize('Không có dự án con', nfc))
    )
),

with_default as (
    select * from deduped
    union all
    select * from default_per_project
)

select distinct on (sub_project_id)
    {{ dbt_utils.generate_surrogate_key(['sub_project_id']) }} as dim_sub_project_sk
    , sub_project_id
    , project_id
    , sub_project_name
    , description
from with_default
order by sub_project_id, project_id, sub_project_name
