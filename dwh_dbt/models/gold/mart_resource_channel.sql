{{ config(materialized='table', schema='gold') }}

with stock_view_revenue as (
    select
        resource_id
        , sum(view) as total_view
        , sum(revenue_amount) as total_revenue
    from {{ ref('fact_revenue_by_resources') }}
    group by resource_id
),

monthly_metrics as (
    select
        resource_id
        , date_trunc('month', recorded_date) as month
        , sum(view) as monthly_view
        , sum(revenue_amount) as monthly_revenue
    from {{ ref('fact_revenue_by_resources') }}
    group by resource_id, date_trunc('month', recorded_date)
),

growth as (
    select distinct on (resource_id)
        resource_id
        , (monthly_view - lag(monthly_view) over (partition by resource_id order by month))
            / nullif(lag(monthly_view) over (partition by resource_id order by month), 0) as growth_view
        , (monthly_revenue - lag(monthly_revenue) over (partition by resource_id order by month))
            / nullif(lag(monthly_revenue) over (partition by resource_id order by month), 0) as growth_revenue
    from monthly_metrics
    order by resource_id, month desc
),

-- cầu nối dim_stock ↔ dim_resource qua x_music_song
stock_resource_bridge as (
    select
        cast(hg_code as text) as hg_stock_id
        , cast(id as text)    as resource_id
    from {{ source('staging', 'x_music_song') }}
    where hg_code is not null
),

channel_video as (
    select
        sv.hg_stock_id
        , count(distinct dv.channel_id) as channel_use
        , count(distinct sv.video_id)   as video_use
    from {{ ref('int_stock_video') }} sv
    join {{ ref('dim_video') }} dv on sv.video_id = dv.video_id
    where dv.published_date is not null
    group by sv.hg_stock_id
)

select
    {{ dbt_utils.generate_surrogate_key(['ds.hg_stock_id']) }} as mart_resource_channel_sk
    , ds.hg_stock_id                                as resource_id
    , ds.name                                       as resource_name
    , dp.project_name                               as project
    , dsp.sub_project_name                          as sub_project
    , dr.acceptance_score
    , case
        when pr.hg_stock_id is not null then 'Thu mua'
        else 'Sản xuất'
      end                                           as resource_type
    , coalesce(svr.total_view, 0)                   as view
    , coalesce(svr.total_revenue, 0)                as revenue
    , g.growth_view
    , g.growth_revenue
    , coalesce(cv.channel_use, 0)                   as channel_use
    , coalesce(cv.video_use, 0)                     as video_use
from {{ ref('dim_stock') }} ds

-- cầu nối sang dim_resource
left join stock_resource_bridge srb on ds.hg_stock_id = srb.hg_stock_id
left join {{ ref('dim_resource') }} dr on srb.resource_id = dr.resource_id

-- chain từ dim_resource → project
left join {{ ref('dim_repository') }} repo on dr.repository_id = repo.repository_id
left join {{ ref('dim_sub_project') }} dsp on repo.sub_project_id = dsp.sub_project_id
left join {{ ref('dim_project') }} dp on dsp.project_id = dp.project_id

left join {{ ref('dim_purchased_resource') }} pr on ds.hg_stock_id = pr.hg_stock_id
left join stock_view_revenue svr on ds.hg_stock_id = svr.resource_id
left join growth g on ds.hg_stock_id = g.resource_id
left join channel_video cv on ds.hg_stock_id = cv.hg_stock_id