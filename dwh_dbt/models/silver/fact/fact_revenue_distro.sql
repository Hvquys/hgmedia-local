with base as (
    select
        retailer,
        "isrc" as isrc,
        nullif(trim(artist),'') as artist,
        to_char(cast("reportingPeriod" as date),'YYYY-MM') as revenue_month
        , cast("reportingPeriod" as date) as reporting_date
        ,cast("revenue" as numeric) as earning
    from {{ source('staging','sale') }}
    where nullif(trim("isrc"),'') is not null
),

aggregated as (
    select
        retailer
        , isrc
        , artist
        , revenue_month
        , min(reporting_date) as first_date
        , sum(earning) as revenue_amount
    from base
    group by retailer, isrc, artist, revenue_month
),

usd as (
    select distinct on (record_date)
        record_date
        , exchange_rate
    from {{ ref('dim_usd') }}
    order by record_date
)

select
    {{ dbt_utils.generate_surrogate_key(['retailer','isrc','revenue_month']) }} as fact_revenue_distro_sk
    , revenue_amount
    , revenue_month
    , nullif(trim(retailer),'') as platform
    , isrc
    , artist
    , u.exchange_rate
from aggregated a
left join usd u on a.first_date = u.record_date