{{ config(materialized='table', schema='gold') }}

-- Mart_Resources_Video_Channel
-- Grain: 1 dòng cho mỗi hg_stock_id (biên tập/resource) ứng với từng video - kênh
--
-- JOIN KEY THỰC TẾ (đã xác nhận qua information_schema):
--   - fact_editing.editing_code   = dim_video.editing_code
--   - dim_video.channel_id        = dim_channel.channel_id
--   - dim_channel.network_id      = dim_net.net_id
--   - dim_channel.company_id      = dim_company_stock.company_id

with editing_resources as (
    -- Fact_Editing: editing_code là key nối sang dim_video (KHÔNG có video_id trong bảng này)
    select
        cast(editing_id as text)   as editing_id
        , cast(editing_code as text) as editing_code
        , cast(hg_stock_id as text)  as hg_stock_id
        , position
        , duration
    from {{ ref('fact_editing') }}
    where hg_stock_id is not null
),

video as (
    select
        cast(video_id as text)      as video_id
        , cast(channel_id as text)  as channel_id
        , cast(editing_code as text) as editing_code
        , video_url
        , video_name
    from {{ ref('dim_video') }}
),

channel as (
    select
        cast(channel_id as text)   as channel_id
        , cast(company_id as text) as company_id
        , cast(network_id as text) as network_id
        , link                     as link_channel
        , employee_name
    from {{ ref('dim_channel') }}
),

net as (
    select
        cast(net_id as text) as net_id
        , net_name
    from {{ ref('dim_net') }}
),

company as (
    select
        cast(company_id as text) as company_id
        , company_name
    from {{ ref('dim_company_stock') }}
)

select
    v.video_url                 as link_video
    , ch.link_channel           as link_channel
    , er.hg_stock_id            as resource_id
    , er.position               as position
    , n.net_name                as network
    , c.company_name            as company
    , cast(null as text)        as department
    , ch.employee_name          as employee
from editing_resources er
join video v
    on er.editing_code = v.editing_code
left join channel ch
    on v.channel_id = ch.channel_id
left join net n
    on ch.network_id = n.net_id
left join company c
    on ch.company_id = c.company_id