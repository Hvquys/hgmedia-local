-- silver.dim_isrc
with distro_isrc as (
    select distinct isrc
    from {{ ref('fact_revenue_distro') }}
    where isrc is not null
),

base as (
    select
        {{ dbt_utils.generate_surrogate_key(['xms.name', 'xms.isrc']) }} as dim_isrc_sk
        , nullif(trim(xms.name), '') as hg_stock_id
        , nullif(trim(xms.isrc), '') as isrc
        , row_number() over (partition by xms.name order by xms.isrc) as rn_stock
        , row_number() over (partition by xms.isrc order by xms.name) as rn_isrc
    from {{ source('staging', 'x_music_song') }} xms
    where xms.active is true
      and nullif(trim(xms.isrc), '') is not null
)

select
    b.dim_isrc_sk
    , b.hg_stock_id
    , b.isrc
from base b
inner join distro_isrc d on b.isrc = d.isrc
where b.rn_stock = 1 and b.rn_isrc = 1