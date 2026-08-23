{{ config(materialized='table') }}

with name_map as (
    select
        lower(
            trim(
                regexp_replace(
                    normalize(trim(alias_key), nfc),
                    '\s+',
                    ' ',
                    'g'
                )
            )
        ) as alias_key
        , nullif(
            trim(
                regexp_replace(
                    normalize(trim(canonical_name), nfc),
                    '\s+',
                    ' ',
                    'g'
                )
            ),
            ''
        ) as canonical_name
    from {{ ref('int_purchase_name_map') }}
),

resource_source as (
    select distinct
        nullif(trim(pr."Mã"), '') as hg_stock_id
        , nullif(trim(pr."Tiêu đề"), '') as resources_name
        , nullif(
            trim(
                regexp_replace(
                    normalize(
                        trim(coalesce(nm.canonical_name, pr."Repository")),
                        nfc
                    ),
                    '\s+',
                    ' ',
                    'g'
                )
            ),
            ''
        ) as partner_name
        , lower(
            trim(
                regexp_replace(
                    normalize(
                        trim(coalesce(nm.canonical_name, pr."Repository")),
                        nfc
                    ),
                    '\s+',
                    ' ',
                    'g'
                )
            )
        ) as partner_key
    from {{ source('staging', 'purchased_resource') }} pr
    left join name_map nm
        on nm.alias_key = lower(
            trim(
                regexp_replace(
                    normalize(trim(pr."Repository"), nfc),
                    '\s+',
                    ' ',
                    'g'
                )
            )
        )
    where trim(pr."Mã") ~ '^HGFA[0-9A-F]{32}$'
        and nullif(trim(pr."Repository"), '') is not null
),

partner_source_prepared as (
    select
        nullif(
            trim(
                regexp_replace(
                    normalize(
                        trim(
                            coalesce(
                                nm_partner.canonical_name
                                , nm_repository.canonical_name
                                , p."Tên đối tác"
                            )
                        ),
                        nfc
                    ),
                    '\s+',
                    ' ',
                    'g'
                )
            ),
            ''
        ) as partner_name
        , lower(
            trim(
                regexp_replace(
                    normalize(
                        trim(
                            coalesce(
                                nm_partner.canonical_name
                                , nm_repository.canonical_name
                                , p."Tên đối tác"
                            )
                        ),
                        nfc
                    ),
                    '\s+',
                    ' ',
                    'g'
                )
            )
        ) as partner_key
        , nullif(trim(p."Ngày ký HĐ"), '') as buy_date
        , nullif(trim(p."Cách tính giá"), '') as repository_type
        , lower(
            trim(
                regexp_replace(
                    normalize(trim(p."Dự án"), nfc),
                    '\s+',
                    ' ',
                    'g'
                )
            )
        ) as project_key
        , p._loaded_at
    from {{ source('staging', 'partners') }} p
    left join name_map nm_partner
        on nm_partner.alias_key = lower(
            trim(
                regexp_replace(
                    normalize(trim(p."Tên đối tác"), nfc),
                    '\s+',
                    ' ',
                    'g'
                )
            )
        )
    left join name_map nm_repository
        on nm_repository.alias_key = lower(
            trim(
                regexp_replace(
                    normalize(trim(p."Tên kho trên HG Stock"), nfc),
                    '\s+',
                    ' ',
                    'g'
                )
            )
        )
    where nullif(trim(p."Tên đối tác"), '') is not null
),

partner_source_candidates as (
    select
        *
        , row_number() over (
            partition by partner_key
            order by _loaded_at desc nulls last
        ) as partner_order
    from partner_source_prepared
    where partner_key is not null
),

partner_source as (
    select
        partner_name
        , partner_key
        , buy_date
        , repository_type
        , project_key
    from partner_source_candidates
    where partner_order = 1
),

unique_project_by_name as (
    select
        project_name_key
        , min(project_id) as project_id
    from (
        select
            project_id
            , lower(
                trim(
                    regexp_replace(
                        normalize(trim(project_name), nfc),
                        '\s+',
                        ' ',
                        'g'
                    )
                )
            ) as project_name_key
        from {{ ref('dim_project') }}
        where nullif(trim(project_name), '') is not null
    ) projects
    where project_name_key is not null
    group by project_name_key
    having count(distinct project_id) = 1
),

default_sub_project as (
    select
        sub_project_id
        , project_id
    from {{ ref('dim_sub_project') }}
    where lower(
        trim(
            regexp_replace(
                normalize(trim(sub_project_name), nfc),
                '\s+',
                ' ',
                'g'
            )
        )
    ) = 'không có dự án con'
),

repository_by_name as (
    select
        repository_id
        , sub_project_id
        , lower(
            trim(
                regexp_replace(
                    normalize(trim(repository_name), nfc),
                    '\s+',
                    ' ',
                    'g'
                )
            )
        ) as repository_name_key
    from {{ ref('dim_repository') }}
    where nullif(trim(repository_name), '') is not null
),

joined as (
    select
        r.hg_stock_id
        , r.resources_name
        , r.partner_name
        , r.partner_key
        , p.buy_date
        , p.repository_type
        , dsp.sub_project_id
        , dr.repository_id
        , row_number() over (
            partition by r.hg_stock_id
            order by
                case
                    when dr.repository_id is not null
                        and dr.sub_project_id = dsp.sub_project_id
                        then 1
                    when dr.repository_id is not null
                        then 2
                    when dsp.sub_project_id is not null
                        then 3
                    else 4
                end
                , r.partner_key
                , r.resources_name nulls last
                , dr.repository_id
        ) as repository_order
    from resource_source r
    left join partner_source p
        on p.partner_key = r.partner_key
    left join unique_project_by_name dp
        on dp.project_name_key = p.project_key
    left join default_sub_project dsp
        on dsp.project_id = dp.project_id
    left join repository_by_name dr
        on dr.repository_name_key = r.partner_key
        and (
            dsp.sub_project_id is null
            or dr.sub_project_id = dsp.sub_project_id
        )
)

select
    {{ dbt_utils.generate_surrogate_key(['hg_stock_id']) }}
        as dim_purchased_resource_sk
    , hg_stock_id
    , resources_name
    , repository_id as repository
    , partner_name
    , partner_key
    , {{ dbt_utils.generate_surrogate_key(['partner_key']) }} as partner_id
    , buy_date
    , repository_type
    , sub_project_id
from joined
where repository_order = 1