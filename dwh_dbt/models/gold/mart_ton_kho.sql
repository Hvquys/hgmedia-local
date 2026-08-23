{{ config(materialized='table') }}

with distributed_music as (
    select
        hg_stock_id
        , max(distribution_date)::date as last_distribution_date
    from {{ ref('fact_distribution') }}
    where nullif(hg_stock_id, '') is not null
        and left(hg_stock_id, 4) = 'HGFA'
    group by hg_stock_id
),

last_used as (
    select
        fe.hg_stock_id
        , max(dv.published_date)::date as last_published_date
    from {{ ref('fact_editing') }} fe
    inner join {{ ref('bridge_bt_vid') }} bv
        on bv.editing_code = fe.editing_code
    inner join {{ ref('dim_video') }} dv
        on dv.video_id = bv.video_id
    where nullif(fe.hg_stock_id, '') is not null
        and left(fe.hg_stock_id, 4) = 'HGFA'
        and nullif(fe.editing_code, '') is not null
        and nullif(bv.video_id, '') is not null
        and dv.published_date is not null
    group by fe.hg_stock_id
),

score_by_stock as (
    select
        st.hg_stock_id
        , max(dr.acceptance_score) as acceptance_score
    from {{ ref('dim_stock') }} st
    left join {{ ref('fact_label_operation') }} flo
        on nullif(st.isrc, '') is not null
        and flo.isrc = st.isrc
    left join {{ ref('dim_resource') }} dr
        on dr.resource_id = flo.resource_id
    where nullif(st.hg_stock_id, '') is not null
        and left(st.hg_stock_id, 4) = 'HGFA'
    group by st.hg_stock_id
),

base as (
    select
        dm.hg_stock_id
        , dm.last_distribution_date
        , lu.last_published_date
        , coalesce(
            lu.last_published_date
            , dm.last_distribution_date
          ) as inventory_start_date
        , (
            current_date
            - coalesce(
                lu.last_published_date
                , dm.last_distribution_date
              )
          )::int as inventory_age_days
        , sbs.acceptance_score
    from distributed_music dm
    left join last_used lu
        on lu.hg_stock_id = dm.hg_stock_id
    left join score_by_stock sbs
        on sbs.hg_stock_id = dm.hg_stock_id
    where coalesce(
        lu.last_published_date
        , dm.last_distribution_date
    ) is not null
)

select
    hg_stock_id
    , last_distribution_date
    , last_published_date
    , inventory_start_date
    , inventory_age_days
    , case
        when inventory_age_days <= 30 then 1
        when inventory_age_days <= 60 then 2
        else 3
      end as inventory_age_sort
    , case
        when inventory_age_days <= 30 then 'Tồn kho 0-30 ngày'
        when inventory_age_days <= 60 then 'Tồn kho 30-60 ngày'
        else 'Tồn kho > 60 ngày'
      end as inventory_age_group
    , acceptance_score
    , case
        when acceptance_score >= 9.75 then 1
        when acceptance_score >= 9.25 then 2
        when acceptance_score >= 8.75 then 3
        when acceptance_score >= 8.25 then 4
        when acceptance_score >= 7.75 then 5
        else 6
      end as score_sort
    , case
        when acceptance_score >= 9.75 then '10 điểm'
        when acceptance_score >= 9.25 then '9.5 điểm'
        when acceptance_score >= 8.75 then '9 điểm'
        when acceptance_score >= 8.25 then '8.5 điểm'
        when acceptance_score >= 7.75 then '8 điểm'
        else 'Khác/chưa có điểm'
      end as score_group
    , case
        when last_published_date is not null then 'Đã từng sử dụng'
        else 'Chưa từng sử dụng'
      end as usage_status
from base
where inventory_age_days >= 0
