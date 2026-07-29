{{ config(materialized='table') }}

with vid_metric as (
    select
        m."YoutubeVideoId" as video_id
        , m."Date"::date as recorded_date
        , sum(m."EstimatedRevenue"::numeric) as revenue
        , sum(m."Views"::numeric) as views
    from {{ source('staging','channel_video_metric') }} m
    where m."YoutubeVideoId" is not null
        and m."Date" is not null
    group by
        m."YoutubeVideoId"
        , m."Date"::date
),

vid_res as (
    select
        b.video_id
        , b.editing_code
        , f.hg_stock_id
        , f.position
        , count(*) over (
            partition by b.video_id, b.editing_code
        ) as n_res
        , row_number() over (
            partition by b.video_id, b.editing_code
            order by f.position
        ) as rn
    from {{ ref('bridge_bt_vid') }} b
    join {{ ref('fact_editing') }} f
        on b.editing_code = f.editing_code
    where b.video_id is not null
        and f.hg_stock_id is not null
),

weighted as (
    select
        vr.video_id
        , vr.editing_code
        , vr.hg_stock_id
        , vr.position
        , vr.n_res
        , vr.rn
        , case
            when vr.n_res >= 5 then (array[5,2,1,1,1])[vr.rn]
            when vr.n_res = 4 then (array[6,2,1,1])[vr.rn]
            when vr.n_res = 3 then (array[6,3,1])[vr.rn]
            when vr.n_res = 2 then (array[7,3])[vr.rn]
            else 10
        end as w
    from vid_res vr
    where vr.rn <= 5
),

usd as (
    select distinct on (record_date)
        record_date
        , exchange_rate
    from {{ ref('dim_usd') }}
    order by record_date
)

select
    {{ dbt_utils.generate_surrogate_key([
        'w.video_id',
        'w.editing_code',
        'w.hg_stock_id',
        'w.position::text',
        "coalesce(m.recorded_date::text, 'no_metric')"
    ]) }} as revenue_id
    , w.video_id
    , w.hg_stock_id as resource_id
    , coalesce(m.revenue, 0) * w.w / 10.0 as revenue_amount
    , m.recorded_date::timestamp as recorded_date
    , coalesce(m.views, 0) * w.w / 10.0 as view
    , u.exchange_rate
from weighted w
left join vid_metric m
    on w.video_id = m.video_id
left join usd u
    on m.recorded_date = u.record_date
