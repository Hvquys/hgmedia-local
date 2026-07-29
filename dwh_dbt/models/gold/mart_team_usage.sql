{{ config(materialized='table', schema='gold') }}

with team_dist as (
    select
        department as team
        , count(hg_stock_id) as distributed_resources
    from {{ ref('fact_distribution') }}
    where department is not null
    group by department
),

published_videos as (
    select video_id
    from {{ ref('dim_video') }}
    where published_date is not null
),

editing_codes as (
    select distinct b.editing_code
    from {{ ref('bridge_bt_vid') }} b
    join published_videos pv on b.video_id = pv.video_id
),

usage_per_team as (
    select
        fd.department as team
        , count(distinct fe.hg_stock_id) as usage_resources
    from {{ ref('fact_distribution') }} fd
    join {{ ref('fact_editing') }} fe on fd.hg_stock_id = fe.hg_stock_id
    join editing_codes ec on fe.editing_code = ec.editing_code
    where fe.hg_stock_id is not null
        and fd.department is not null
    group by fd.department
),

view_per_team as (
    select
        fd.department as team
        , sum(fv.view_count) as total_view
    from {{ ref('fact_distribution') }} fd
    join {{ ref('int_stock_video') }} sv on fd.hg_stock_id = sv.hg_stock_id
    join {{ ref('fact_view_yt') }} fv on sv.video_id = fv.video_id
    where fd.department is not null
    group by fd.department
),

revenue_per_team as (
    select
        fd.department as team
        , sum(fr.revenue_amount) as total_revenue
    from {{ ref('fact_distribution') }} fd
    join {{ ref('int_stock_video') }} sv on fd.hg_stock_id = sv.hg_stock_id
    join {{ ref('fact_revenue_yt') }} fr on sv.video_id = fr.video_id
    where fd.department is not null
    group by fd.department
),

monthly_view as (
    select
        fd.department as team
        , date_trunc('month', fv.recorded_date) as month
        , sum(fv.view_count) as monthly_view
    from {{ ref('fact_distribution') }} fd
    join {{ ref('int_stock_video') }} sv on fd.hg_stock_id = sv.hg_stock_id
    join {{ ref('fact_view_yt') }} fv on sv.video_id = fv.video_id
    where fd.department is not null
    group by fd.department, date_trunc('month', fv.recorded_date)
),

monthly_revenue as (
    select
        fd.department as team
        , date_trunc('month', fr.revenue_date) as month
        , sum(fr.revenue_amount) as monthly_revenue
    from {{ ref('fact_distribution') }} fd
    join {{ ref('int_stock_video') }} sv on fd.hg_stock_id = sv.hg_stock_id
    join {{ ref('fact_revenue_yt') }} fr on sv.video_id = fr.video_id
    where fd.department is not null
    group by fd.department, date_trunc('month', fr.revenue_date)
),

growth_view as (
    select distinct on (team)
        team
        , (monthly_view - lag(monthly_view) over (partition by team order by month))
            / nullif(lag(monthly_view) over (partition by team order by month), 0) as growth_view
    from monthly_view
    order by team, month desc
),

growth_revenue as (
    select distinct on (team)
        team
        , (monthly_revenue - lag(monthly_revenue) over (partition by team order by month))
            / nullif(lag(monthly_revenue) over (partition by team order by month), 0) as growth_revenue
    from monthly_revenue
    order by team, month desc
)

select
    {{ dbt_utils.generate_surrogate_key(['td.team']) }} as mart_team_usage_sk
    , td.team
    , td.distributed_resources
    , coalesce(u.usage_resources, 0) as usage_resources
    , coalesce(vt.total_view, 0) as view
    , coalesce(rt.total_revenue, 0) as revenue
    , gv.growth_view
    , gr.growth_revenue
from team_dist td
left join usage_per_team u on td.team = u.team
left join view_per_team vt on td.team = vt.team
left join revenue_per_team rt on td.team = rt.team
left join growth_view gv on td.team = gv.team
left join growth_revenue gr on td.team = gr.team