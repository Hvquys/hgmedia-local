{{ config(materialized='table', schema='gold') }}

with produced_resources as (
    select distinct
        cast(hg_stock_id as text) as hg_stock_id
        , cast(repository_id as text) as repository_id
        , cast(po_detail_id as text) as po_detail_id
    from {{ ref('dim_resources') }}
    where resource_source = 'odoo'
        and hg_stock_id is not null
),

purchase_order_confirmed as (
    select
        cast(pol.id as text) as po_detail_id
        , min(po.date_approved) as po_confirmed_date
    from {{ source('staging', 'purchase_order_line') }} pol
    join {{ source('staging', 'purchase_order') }} po
        on cast(pol.order_id as text) = cast(po.id as text)
    where po.date_approved is not null
    group by cast(pol.id as text)
),

resource_po_confirmed as (
    select
        pr.hg_stock_id
        , min(poc.po_confirmed_date) as po_confirmed_date
    from produced_resources pr
    left join purchase_order_confirmed poc
        on pr.po_detail_id = poc.po_detail_id
    group by pr.hg_stock_id
),

produced_stock_repository as (
    select distinct
        hg_stock_id
        , repository_id
    from produced_resources
),

distribution_sla as (
    select
        psr.repository_id
        , avg(
            case
                when rpc.po_confirmed_date is not null
                    and fd.distribution_date is not null
                    then (fd.distribution_date::date - rpc.po_confirmed_date::date)::numeric
            end
          ) as sla_ord_distribution
    from {{ ref('fact_distribution') }} fd
    join produced_stock_repository psr
        on cast(fd.hg_stock_id as text) = psr.hg_stock_id
    left join resource_po_confirmed rpc
        on psr.hg_stock_id = rpc.hg_stock_id
    group by psr.repository_id
),

resource_count as (
    select
        repository_id
        , count(distinct hg_stock_id) as number_resources
    from produced_stock_repository
    group by repository_id
)

select
    cast(repo.repository_id as text)                as repository_id
    , dsp.sub_project_name                          as sub_project
    , dp.project_name                               as project
    , rc.number_resources                           as number_resources
    , nullif(dsla.sla_ord_distribution, 0)          as sla_ord_distribution
from {{ ref('dim_repository') }} repo
left join {{ ref('dim_sub_project') }} dsp
    on cast(repo.sub_project_id as text) = cast(dsp.sub_project_id as text)
left join {{ ref('dim_project') }} dp
    on cast(dsp.project_id as text) = cast(dp.project_id as text)
left join resource_count rc
    on cast(repo.repository_id as text) = rc.repository_id
left join distribution_sla dsla
    on cast(repo.repository_id as text) = dsla.repository_id
where rc.number_resources > 0
