{{ config(materialized='table') }}

with name_map as (
    select * from {{ ref('int_purchase_name_map') }}
),

cost as (
    select
        coalesce(nm.canonical_name, trim(c."Tên đối tác")) as partner_name
        , lower(coalesce(nm.canonical_name, trim(c."Tên đối tác"))) as partner_key
        , cast(trim(c."_year") as integer) as year_no
        , c."Total năm" as total_year_raw
        {% for m in range(1, 13) %}
        , c."CP tháng {{ m }}" as month_{{ m }}_cost_raw
        {% endfor %}
    from {{ source('staging', 'purchase_cost') }} c
    left join name_map nm
        on nm.alias_key = lower(
            regexp_replace(
                trim(c."Tên đối tác")
                , '[[:space:]]+'
                , ' '
                , 'g'
            )
        )
    where trim(c."_year") ~ '^[0-9]{4}$'
        and nullif(trim(c."Tên đối tác"), '') is not null
),

res as (
    select
        d.dim_purchased_resource_sk
        , d.hg_stock_id
        , d.partner_id
        , d.partner_name
        , d.partner_key
        , case
            when nullif(trim(d.buy_date), '') is not null
                then to_date(trim(d.buy_date), 'DD/MM/YYYY')
            else null
        end as buy_date
    from {{ ref('dim_purchased_resource') }} d
    where trim(d.hg_stock_id) ~ '^HGFA[0-9A-F]{32}$'
        and nullif(trim(d.partner_key), '') is not null
),

calendar_months as (
    select
        y.year_no
        , m.month_no
        , make_date(y.year_no, m.month_no, 1) as month_start
        , (
            make_date(y.year_no, m.month_no, 1)
            + interval '1 month - 1 day'
        )::date as month_end
    from (
        select distinct year_no from cost
    ) y
    cross join generate_series(1, 12) as m(month_no)
),

resource_months as (
    select
        r.dim_purchased_resource_sk
        , r.hg_stock_id
        , r.partner_id
        , r.partner_name
        , r.partner_key
        , r.buy_date
        , c.year_no
        , c.month_no
        , c.month_start
        , c.month_end
    from res r
    cross join calendar_months c
    where r.buy_date is null
        or c.month_start >= date_trunc('month', r.buy_date)::date
),

res_count as (
    select
        partner_key
        , year_no
        , month_no
        , count(distinct dim_purchased_resource_sk) as n_res
    from resource_months
    group by partner_key, year_no, month_no
),

unpivoted as (
    {% for m in range(1, 13) %}
    select
        c.partner_name
        , c.partner_key
        , c.year_no
        , {{ m }} as month_no
        , c.total_year_raw
        , c.month_{{ m }}_cost_raw as month_cost_raw
    from cost c
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

parsed as (
    select
        partner_key
        , year_no
        , month_no
        , sum(
            coalesce(
                nullif(regexp_replace(total_year_raw, '[^0-9]', '', 'g'), '')::numeric
                , 0
            )
        ) as total_year_num
        , sum(
            coalesce(
                nullif(regexp_replace(month_cost_raw, '[^0-9]', '', 'g'), '')::numeric
                , 0
            )
        ) as month_cost_num
    from unpivoted
    group by partner_key, year_no, month_no
)

select
    {{ dbt_utils.generate_surrogate_key([
        'rm.dim_purchased_resource_sk',
        'rm.year_no',
        'rm.month_no'
    ]) }} as cost_id
    , rm.dim_purchased_resource_sk as purchased_resource_sk
    , rm.hg_stock_id as resource_id
    , rm.partner_id
    , rm.partner_name
    , case
        when coalesce(p.month_cost_num, 0) > 0
            then (p.total_year_num / nullif(rc.n_res, 0)) / 25000.0
        else 0
    end as total_cost
    , case
        when coalesce(p.month_cost_num, 0) > 0
            then (p.month_cost_num / nullif(rc.n_res, 0)) / 25000.0
        else 0
    end as additional_cost
    , rm.month_end as incurred_datetime
from resource_months rm
join res_count rc
    on rc.partner_key = rm.partner_key
    and rc.year_no = rm.year_no
    and rc.month_no = rm.month_no
left join parsed p
    on p.partner_key = rm.partner_key
    and p.year_no = rm.year_no
    and p.month_no = rm.month_no

