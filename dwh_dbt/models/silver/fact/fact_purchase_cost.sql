{{ config(materialized='table') }}

with cost as (
    select
        *
        , "_year" as year_no
    from {{ source('staging', 'purchase_cost') }}
),

kho as (
    select distinct
        trim("Tên đối tác") as partner_name
        , trim("Tên kho trên HG Stock") as repo_name
    from {{ source('staging', 'partners') }}
    where nullif(trim("Tên kho trên HG Stock"), '') is not null
),

res as (
    select distinct
        d.hg_stock_id
        , trim(s."Repository") as repo_name
    from {{ ref('dim_purchased_resource') }} d
    join {{ source('staging', 'purchased_resource') }} s
        on d.hg_stock_id = trim(s."Mã")
    where d.hg_stock_id ~ '^HGFA[0-9A-F]{32}$'
        and nullif(trim(s."Repository"), '') is not null
),

res_count as (
    select
        repo_name
        , count(distinct hg_stock_id) as n_res
    from res
    group by repo_name
),

unpivoted as (
    {% for m in range(1, 13) %}
    select
        trim(c."Tên đối tác") as partner_name
        , c.year_no
        , {{ m }} as month_no
        , c."Total năm" as total_year_raw
        , c."CP tháng {{ m }}" as month_cost_raw
    from cost c
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

parsed as (
    select
        u.partner_name
        , u.year_no
        , u.month_no
        , coalesce(
            cast(nullif(regexp_replace(u.total_year_raw, '[^0-9]', '', 'g'), '') as numeric)
            , 0
        ) as total_year_num
        , coalesce(
            cast(nullif(regexp_replace(u.month_cost_raw, '[^0-9]', '', 'g'), '') as numeric)
            , 0
        ) as month_cost_num
    from unpivoted u
)

select
    {{ dbt_utils.generate_surrogate_key([
        'res.hg_stock_id',
        'p.year_no',
        'p.month_no'
    ]) }} as cost_id
    , res.hg_stock_id as resource_id

    , case
        when p.month_cost_num > 0
            then (p.total_year_num / nullif(rc.n_res, 0)) / 25000.0
        else 0
    end as total_cost

    , case
        when p.month_cost_num > 0
            then (p.month_cost_num / nullif(rc.n_res, 0)) / 25000.0
        else 0
    end as additional_cost

    , (
        date_trunc('month', make_date(p.year_no::int, p.month_no, 1))
        + interval '1 month - 1 day'
    )::date as incurred_datetime

from parsed p
join kho k
    on p.partner_name = k.partner_name
join res_count rc
    on rc.repo_name = k.repo_name
join res
    on res.repo_name = k.repo_name