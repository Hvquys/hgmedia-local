{{ config(materialized='table') }}

with base as (
    select
        retailer,
        "isrc" as isrc,
        nullif(trim(artist), '') as artist,
        to_char(cast("reportingPeriod" as date), 'YYYY-MM') as revenue_month,
        cast("reportingPeriod" as date) as reporting_date,
        cast("revenue" as numeric) as earning
    from {{ source('staging','sale') }}
    where nullif(trim("isrc"), '') is not null
),

revenue_agg as (
    select
        retailer,
        isrc,
        artist,
        revenue_month,
        min(reporting_date) as first_date,
        sum(earning) as revenue_amount
    from base
    group by retailer, isrc, artist, revenue_month
),

stream_agg as (
    select
        nullif(trim(isrc), '') as isrc,
        to_char(cast(nullif(trim("reportingDate"), '') as date), 'YYYY-MM') as revenue_month,
        sum(replace(cast("dailyStream" as text), ',', '')::numeric) as stream_count
    from {{ source('staging','stream_distro') }}
    where nullif(trim(isrc), '') is not null
        and nullif(trim("reportingDate"), '') is not null
    group by
        nullif(trim(isrc), ''),
        to_char(cast(nullif(trim("reportingDate"), '') as date), 'YYYY-MM')
),

usd as (
    select distinct on (record_date)
        record_date,
        exchange_rate
    from {{ ref('dim_usd') }}
    order by record_date
)

select
    {{ dbt_utils.generate_surrogate_key([
        'r.retailer',
        'r.isrc',
        'r.revenue_month'
    ]) }} as fact_revenue_stream_distro_sk,

    r.revenue_amount,
    coalesce(s.stream_count, 0) as stream_count,

    case
        when coalesce(s.stream_count, 0) = 0 then 0
        else r.revenue_amount / s.stream_count * 1000
    end as rpm,

    r.revenue_month,
    nullif(trim(r.retailer), '') as platform,
    r.isrc,
    r.artist,
    u.exchange_rate

from revenue_agg r
left join stream_agg s
    on r.isrc = s.isrc
    and r.revenue_month = s.revenue_month
left join usd u
    on r.first_date = u.record_date
