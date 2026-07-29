-- silver.fact_distribution
select
    {{ dbt_utils.generate_surrogate_key(['d."Id"']) }} as fact_distribution_sk
    , d."Id" as distribution_id
    , rfi."ResourceFileId" as hg_stock_id
    , cast(d."CreatedDate" as timestamp) as distribution_date
    , 'Dùng chung' as recipient_company
    , dep.dept_names as department
    , grp.team_names as team
    , usr.emp_names as employee
    , d."CreatedByUserId" as distributed_employee_id
from {{ source('staging','distribution_media_history') }} d
inner join {{ source('staging','resource_file_info') }} rfi
    on d."ResourceFileInfoId" = rfi."Id"
    and rfi."ResourceFileId" like 'HGFA%'   -- chỉ lấy tài nguyên có mã bắt đầu bằng HGFA

left join lateral (
    select string_agg(dd."Name", ', ') as dept_names
    from unnest(string_to_array(nullif(d."DepartmentData",''), ',')) g(id)
    join {{ source('staging','department') }} dd on trim(g.id) = dd."Id"::text
) dep on true

left join lateral (
    select string_agg(gg."Name", ', ') as team_names
    from unnest(string_to_array(nullif(d."GroupData",''), ',')) g(id)
    join {{ source('staging','groups') }} gg on trim(g.id) = gg."Id"::text
) grp on true

left join lateral (
    select string_agg(uu."DisplayName", ', ') as emp_names
    from unnest(string_to_array(nullif(d."UserData",''), ',')) g(id)
    join {{ source('staging','users') }} uu on trim(g.id) = uu."Id"::text
) usr on true