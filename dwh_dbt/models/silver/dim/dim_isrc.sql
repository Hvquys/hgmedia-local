-- models/silver/dim_isrc.sql
-- Adds legacy mappings from staging.resource_before_odoo:
--   "Mã bài" -> hg_stock_id (song code)
--   "ISRC"    -> isrc

with distro_isrc as (
    select distinct nullif(trim(isrc), '') as isrc
    from {{ ref('fact_revenue_distro') }}
    where nullif(trim(isrc), '') is not null
),

x_music_song as (
    select distinct
        nullif(trim(xms.name), '') as hg_stock_id,
        nullif(trim(xms.isrc), '') as isrc,
        1 as source_priority
    from {{ source('staging', 'x_music_song') }} xms
    where xms.active is true
      and nullif(trim(xms.name), '') is not null
      and nullif(trim(xms.isrc), '') is not null
),

resource_before_odoo as (
    select distinct
        nullif(trim(rbo."Mã bài"), '') as hg_stock_id,
        nullif(trim(rbo."ISRC"), '') as isrc,
        2 as source_priority
    from {{ source('staging', 'resource_before_odoo') }} rbo
    where nullif(trim(rbo."Mã bài"), '') is not null
      and nullif(trim(rbo."ISRC"), '') is not null
),

source_mappings as (
    select * from x_music_song
    union all
    select * from resource_before_odoo
),

base as (
    select
        {{ dbt_utils.generate_surrogate_key(['hg_stock_id', 'isrc']) }} as dim_isrc_sk,
        hg_stock_id,
        isrc,
        row_number() over (
            partition by hg_stock_id
            order by source_priority, isrc
        ) as rn_stock,
        row_number() over (
            partition by isrc
            order by source_priority, hg_stock_id
        ) as rn_isrc
    from source_mappings
)

select
    b.dim_isrc_sk,
    b.hg_stock_id,
    b.isrc
from base b
inner join distro_isrc d on b.isrc = d.isrc
where b.rn_stock = 1
  and b.rn_isrc = 1
