-- silver.dim_order_employee
-- Grain: 1 dòng / res_users.id có phát sinh Purchase Order

with po_users as (

    select distinct
        user_id
    from {{ source('staging', 'purchase_order') }}
    where user_id is not null

),

ranked_employee as (

    select
        he.id
        , he.user_id
        , he.department_id
        , he.name
        , he.job_title
        , he.company_id
        , row_number() over (
            partition by he.user_id
            order by he.id
        ) as rn
    from {{ source('staging', 'hr_employee') }} he
    where he.user_id is not null

),

base as (

    select
        {{ dbt_utils.generate_surrogate_key(['hu.id']) }} as dim_order_employee_sk
        , nullif(trim(cast(he.id as text)), '') as order_employee_id
        , nullif(trim(cast(hu.id as text)), '') as employee_id
        , nullif(trim(cast(he.department_id as text)), '') as department_id
        , nullif(trim(cast(he.name as text)), '') as employee_name
        , null as team_name
        , nullif(trim(cast(he.job_title as text)), '') as position
        , nullif(trim(cast(he.company_id as text)), '') as company_id

    from po_users po

    inner join {{ source('staging', 'res_users') }} hu
        on po.user_id = hu.id

    left join ranked_employee he
        on hu.id = he.user_id
        and he.rn = 1
)

select
    dim_order_employee_sk
    , order_employee_id
    , employee_id
    , department_id
    , employee_name
    , team_name
    , position
    , company_id
from base