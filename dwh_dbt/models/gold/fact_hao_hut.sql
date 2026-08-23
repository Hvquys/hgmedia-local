{{ config(materialized='table') }}

with repository_ctx as (
    select
        r.repository_id
        , r.repository_name
        , r.sub_project_id
        , sp.project_id
    from {{ ref('dim_repository') }} r
    left join {{ ref('dim_sub_project') }} sp
        on sp.sub_project_id = r.sub_project_id
),

so_ctx as (
    select
        so.so_id
        , so.po_id
        , po.ordering_company as order_company_id
        , so.so_created_date
        , so.so_confirmed_date
        , po.po_created_date
        , po.po_confirmed_date
    from {{ ref('dim_so') }} so
    left join {{ ref('dim_po') }} po
        on po.po_id = so.po_id
),

od_sx_rows as (
    select
        1 as step_order
        , 'OD->SX' as buoc
        , v.status_order
        , v.tinh_trang
        , concat(
            'OD_SX:'
            , v.status_order
            , ':'
            , coalesce(fsd.so_detail_id, fsd.so_id, fsd.repository, 'unknown')
          ) as flow_item_key
        , cast(null as text) as resource_id
        , cast(null as text) as hg_stock_id
        , cast(null as text) as isrc
        , fsd.so_id
        , so.po_id
        , cast(null as text) as po_detail_id
        , cast(null as text) as channel_id
        , nullif(fsd.repository, '') as repository_id
        , rc.sub_project_id as production_sub_project_id
        , rc.project_id as production_project_id
        , cast(null as text) as stock_sub_project_id
        , cast(null as text) as stock_project_id
        , so.order_company_id
        , cast(null as text) as stock_company_id
        , so.order_company_id as company_id
        , rc.sub_project_id as sub_project_id
        , rc.project_id as project_id
        , cast(null as text) as net_id
        , cast(null as text) as platform
        , cast(null as timestamp) as stock_stored_date
        , cast(null as timestamp) as recorded_date
        , v.so_luong::numeric as so_luong
    from {{ ref('fact_so_detail') }} fsd
    left join repository_ctx rc
        on rc.repository_id = nullif(fsd.repository, '')
    left join so_ctx so
        on so.so_id = fsd.so_id
    cross join lateral (
        values
            (
                1
                , 'Hoàn thành'
                , least(
                    coalesce(fsd.production_qty, 0)
                    , coalesce(fsd.song_qty, 0)
                  )
            )
            , (
                2
                , 'Chưa hoàn thành'
                , greatest(
                    coalesce(fsd.song_qty, 0) - coalesce(fsd.production_qty, 0)
                    , 0
                  )
            )
    ) as v(status_order, tinh_trang, so_luong)
),

pp_resources as (
    select
        resource_id
        , min(nullif(isrc, '')) as isrc
    from {{ ref('fact_label_operation') }}
    where nullif(resource_id, '') is not null
        and (
            release_status = 'sent'
            or release_date is not null
        )
    group by resource_id
),

resource_hg_map as (
    select
        nullif(odoo_id, '') as resource_id
        , max(nullif(hg_stock_id, '')) as hg_stock_id
    from {{ ref('dim_resources') }}
    where nullif(odoo_id, '') is not null
    group by nullif(odoo_id, '')
),

stock_by_hg as (
    select
        hg_stock_id
        , max(nullif(isrc, '')) as isrc
        , min(stock_stored_date) as stock_stored_date
        , max(status) filter (where status = 'Sử dụng') as used_status
        , max(status) as any_status
    from {{ ref('dim_stock') }}
    where nullif(hg_stock_id, '') is not null
    group by hg_stock_id
),

stock_by_isrc as (
    select
        isrc
        , (
            array_agg(
                hg_stock_id
                order by
                    case when status = 'Sử dụng' then 0 else 1 end
                    , stock_stored_date nulls last
                    , hg_stock_id
            )
          )[1] as hg_stock_id
        , min(stock_stored_date) as stock_stored_date
        , max(status) filter (where status = 'Sử dụng') as used_status
        , max(status) as any_status
    from {{ ref('dim_stock') }}
    where nullif(isrc, '') is not null
        and nullif(hg_stock_id, '') is not null
    group by isrc
),

resource_ctx as (
    select distinct
        r.resource_id
        , r.status
        , r.so_id
        , so.po_id
        , r.po_detail_id
        , r.production_plan_detail_id
        , r.repository_id
        , rc.sub_project_id as production_sub_project_id
        , rc.project_id as production_project_id
        , so.order_company_id
        , coalesce(rhm.hg_stock_id, sbisrc.hg_stock_id) as hg_stock_id
        , coalesce(pp.isrc, sbhg.isrc, sbisrc.isrc) as isrc
        , coalesce(sbhg.stock_stored_date, sbisrc.stock_stored_date) as stock_stored_date
        , coalesce(
            sbhg.used_status
            , sbisrc.used_status
            , sbhg.any_status
            , sbisrc.any_status
          ) as stock_status
        , pp.resource_id is not null as is_published
    from {{ ref('dim_resource') }} r
    left join repository_ctx rc
        on rc.repository_id = r.repository_id
    left join so_ctx so
        on so.so_id = r.so_id
    left join pp_resources pp
        on pp.resource_id = r.resource_id
    left join resource_hg_map rhm
        on rhm.resource_id = r.resource_id
    left join stock_by_hg sbhg
        on sbhg.hg_stock_id = rhm.hg_stock_id
    left join stock_by_isrc sbisrc
        on sbisrc.isrc = pp.isrc
    where nullif(r.resource_id, '') is not null
),

youtube_stock_ctx as (
    select distinct
        y.hg_stock_id
        , y.channel_id
        , ch.company_id as stock_company_id
        , ch.project_id as stock_project_id
        , ch.sub_project_id as stock_sub_project_id
        , coalesce(ch.network_id, y.net) as net_id
    from {{ ref('fact_youtube_operation') }} y
    left join {{ ref('dim_channel') }} ch
        on ch.channel_id = y.channel_id
    where nullif(y.hg_stock_id, '') is not null
),

platform_by_stock as (
    select distinct
        s.hg_stock_id
        , nullif(rd.platform, '') as platform
    from {{ ref('dim_stock') }} s
    inner join {{ ref('fact_revenue_distro') }} rd
        on rd.isrc = s.isrc
    where nullif(s.hg_stock_id, '') is not null
        and nullif(rd.platform, '') is not null

    union

    select distinct
        s.hg_stock_id
        , nullif(vs.platform, '') as platform
    from {{ ref('dim_stock') }} s
    inner join {{ ref('fact_view_stream_distro') }} vs
        on vs.isrc = s.isrc
    where nullif(s.hg_stock_id, '') is not null
        and nullif(vs.platform, '') is not null
),

stock_filter_ctx as (
    select distinct
        coalesce(yt.hg_stock_id, pf.hg_stock_id) as hg_stock_id
        , yt.channel_id
        , yt.stock_company_id
        , yt.stock_project_id
        , yt.stock_sub_project_id
        , yt.net_id
        , pf.platform
    from youtube_stock_ctx yt
    full join platform_by_stock pf
        on pf.hg_stock_id = yt.hg_stock_id
),

stock_result as (
    select
        resource_id as hg_stock_id
        , min(recorded_date) as recorded_date
    from {{ ref('fact_revenue_by_resources') }}
    where nullif(resource_id, '') is not null
        and (
            coalesce("view", 0) > 0
            or coalesce(revenue_amount, 0) > 0
        )
    group by resource_id
),

sx_nt_rows as (
    select
        2 as step_order
        , 'SX->NT' as buoc
        , case when rc.status = 'Đã nghiệm thu' then 1 else 2 end as status_order
        , case when rc.status = 'Đã nghiệm thu' then 'Hoàn thành' else 'Chưa hoàn thành' end as tinh_trang
        , concat(
            'SX_NT:'
            , case when rc.status = 'Đã nghiệm thu' then 1 else 2 end
            , ':'
            , rc.resource_id
          ) as flow_item_key
        , rc.resource_id
        , rc.hg_stock_id
        , rc.isrc
        , rc.so_id
        , rc.po_id
        , rc.po_detail_id
        , sf.channel_id
        , rc.repository_id
        , rc.production_sub_project_id
        , rc.production_project_id
        , sf.stock_sub_project_id
        , sf.stock_project_id
        , rc.order_company_id
        , sf.stock_company_id
        , coalesce(sf.stock_company_id, rc.order_company_id) as company_id
        , coalesce(sf.stock_sub_project_id, rc.production_sub_project_id) as sub_project_id
        , coalesce(sf.stock_project_id, rc.production_project_id) as project_id
        , sf.net_id
        , sf.platform
        , rc.stock_stored_date
        , cast(null as timestamp) as recorded_date
        , 1::numeric as so_luong
    from resource_ctx rc
    left join stock_filter_ctx sf
        on sf.hg_stock_id = rc.hg_stock_id
),

nt_pp_rows as (
    select
        3 as step_order
        , 'NT->PP' as buoc
        , case when rc.is_published then 1 else 2 end as status_order
        , case when rc.is_published then 'Hoàn thành' else 'Chưa hoàn thành' end as tinh_trang
        , concat(
            'NT_PP:'
            , case when rc.is_published then 1 else 2 end
            , ':'
            , rc.resource_id
          ) as flow_item_key
        , rc.resource_id
        , rc.hg_stock_id
        , rc.isrc
        , rc.so_id
        , rc.po_id
        , rc.po_detail_id
        , sf.channel_id
        , rc.repository_id
        , rc.production_sub_project_id
        , rc.production_project_id
        , sf.stock_sub_project_id
        , sf.stock_project_id
        , rc.order_company_id
        , sf.stock_company_id
        , coalesce(sf.stock_company_id, rc.order_company_id) as company_id
        , coalesce(sf.stock_sub_project_id, rc.production_sub_project_id) as sub_project_id
        , coalesce(sf.stock_project_id, rc.production_project_id) as project_id
        , sf.net_id
        , sf.platform
        , rc.stock_stored_date
        , cast(null as timestamp) as recorded_date
        , 1::numeric as so_luong
    from resource_ctx rc
    left join stock_filter_ctx sf
        on sf.hg_stock_id = rc.hg_stock_id
    where rc.status = 'Đã nghiệm thu'
),

pp_sd_rows as (
    select
        4 as step_order
        , 'PP->SD' as buoc
        , case when rc.stock_status = 'Sử dụng' then 1 else 2 end as status_order
        , case when rc.stock_status = 'Sử dụng' then 'Hoàn thành' else 'Chưa hoàn thành' end as tinh_trang
        , concat(
            'PP_SD:'
            , case when rc.stock_status = 'Sử dụng' then 1 else 2 end
            , ':'
            , rc.resource_id
          ) as flow_item_key
        , rc.resource_id
        , rc.hg_stock_id
        , rc.isrc
        , rc.so_id
        , rc.po_id
        , rc.po_detail_id
        , sf.channel_id
        , rc.repository_id
        , rc.production_sub_project_id
        , rc.production_project_id
        , sf.stock_sub_project_id
        , sf.stock_project_id
        , rc.order_company_id
        , sf.stock_company_id
        , coalesce(sf.stock_company_id, rc.order_company_id) as company_id
        , coalesce(sf.stock_sub_project_id, rc.production_sub_project_id) as sub_project_id
        , coalesce(sf.stock_project_id, rc.production_project_id) as project_id
        , sf.net_id
        , sf.platform
        , rc.stock_stored_date
        , cast(null as timestamp) as recorded_date
        , 1::numeric as so_luong
    from resource_ctx rc
    left join stock_filter_ctx sf
        on sf.hg_stock_id = rc.hg_stock_id
    where rc.is_published
),

sd_kq_rows as (
    select
        5 as step_order
        , 'SD->KQ' as buoc
        , case when sr.hg_stock_id is not null then 1 else 2 end as status_order
        , case when sr.hg_stock_id is not null then 'Hoàn thành' else 'Chưa hoàn thành' end as tinh_trang
        , concat(
            'SD_KQ:'
            , case when sr.hg_stock_id is not null then 1 else 2 end
            , ':'
            , rc.resource_id
          ) as flow_item_key
        , rc.resource_id
        , rc.hg_stock_id
        , rc.isrc
        , rc.so_id
        , rc.po_id
        , rc.po_detail_id
        , sf.channel_id
        , rc.repository_id
        , rc.production_sub_project_id
        , rc.production_project_id
        , sf.stock_sub_project_id
        , sf.stock_project_id
        , rc.order_company_id
        , sf.stock_company_id
        , coalesce(sf.stock_company_id, rc.order_company_id) as company_id
        , coalesce(sf.stock_sub_project_id, rc.production_sub_project_id) as sub_project_id
        , coalesce(sf.stock_project_id, rc.production_project_id) as project_id
        , sf.net_id
        , sf.platform
        , rc.stock_stored_date
        , sr.recorded_date
        , 1::numeric as so_luong
    from resource_ctx rc
    left join stock_filter_ctx sf
        on sf.hg_stock_id = rc.hg_stock_id
    left join stock_result sr
        on sr.hg_stock_id = rc.hg_stock_id
    where rc.is_published
        and rc.stock_status = 'Sử dụng'
        and nullif(rc.hg_stock_id, '') is not null
),

flow_rows as (
    select * from od_sx_rows
    union all
    select * from sx_nt_rows
    union all
    select * from nt_pp_rows
    union all
    select * from pp_sd_rows
    union all
    select * from sd_kq_rows
)

select
    step_order
    , buoc
    , status_order
    , tinh_trang
    , flow_item_key
    , resource_id
    , hg_stock_id
    , isrc
    , so_id
    , po_id
    , po_detail_id
    , channel_id
    , repository_id
    , production_sub_project_id
    , production_project_id
    , stock_sub_project_id
    , stock_project_id
    , sub_project_id
    , project_id
    , order_company_id
    , stock_company_id
    , company_id
    , net_id
    , platform
    , stock_stored_date
    , recorded_date
    , so_luong
from flow_rows
where coalesce(so_luong, 0) <> 0
