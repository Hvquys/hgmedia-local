with raw_repository_partner_map as (
    select *
    from (
        values
            ('Deep Disco', 'Deep Disco.xlsm')
            , ('Deep Disco', 'DEEP DISCO')
            , ('Deep Disco', 'Blink - BD - Deep House (Deep Disco)')
            , ('Das Ohr Digital', 'Chung (MKT3 cũ).xlsm')
            , ('Das Ohr Digital', 'TECH HOUSE')
            , ('Martynas Laurinavicius / Whitesand', 'Whitesand.xlsx')
            , ('Martynas Laurinavicius / Whitesand', 'Martynas Laurinavicius Whitesand')
            , ('Ardie Son', 'Aron Van Selm & Ardie Son')
            , ('PUEBLO VISTA POLYMESA KAI MOUSIKI LP', 'PVMM.xlsx')
            , ('PUEBLO VISTA POLYMESA KAI MOUSIKI LP', 'PVMM')
            , ('Selah Instrumental Music', 'Thu mua đối tác (Selah Instrumental Music).xlsm')
            , ('Selah Instrumental Music', 'Thu mua đối tác')
            , ('Breath Of Heaven', 'Thu mua đối tác (Breath Of Heaven).xlsx')
            , ('Breath Of Heaven', 'Thu mua đối tác')
            , ('Sergei Grishuk (Сергей Грищук)', 'Relax Nga - MKT1.xlsx')
            , ('Sergei Grishuk (Сергей Грищук)', 'Relax Nga - MKT1')
            , ('Sergei Grishuk (Сергей Грищук)', 'Sergei Grishuk (Сергей Грищук) Relax Nga')
            , ('Oleg Gorbonosov', 'Relax Nga - MKT1.xlsx')
            , ('Melnyk Ihor Olegovich', 'Relax Nga - MKT1.xlsx')
            , ('Tymur Khakimov', 'Relax Nga - MKT1.xlsx')
            , ('Palash Sunvaiya', 'Relax Nga - MKT1.xlsx')
            , ('Nikolay Statilko', 'Relax Nga - MKT1.xlsx')
            , ('Сергей Грищук (Sergey Grischuk)', 'Relax Nga - MKT1.xlsx')
            , ('Orangery', null)
            , ('Aron van Selm', 'Aron Van Selm & Ardie Son')
            , ('Sensoria', null)
            , ('Maniana', 'Maniana Records.xlsm')
            , ('Different Twins', 'Different Twins.xlsx')
            , ('Road Story Records', 'Road Story Records.xlsx')
            , ('Spectrum Recordings', 'Spectrum Recordings.xlsx')
            , ('Three Dot House', 'TECH HOUSE.xlsx')
            , ('Three Dot House', 'TECH HOUSE.xlsm')
            , ('Zero claim records', null)
            , ('Beyond music', 'Inside records.xlsx')
            , ('Beyond music', 'Inside Record')
            , ('Natural Deep (RA music)', 'Natural Deep (RA Music).xlsx')
            , ('Natural Deep (RA music)', 'Natural Deep (RA music)')
            , ('Day dose of house', 'Day Dose Of House.xlsm')
            , ('Deep strip', 'Deep Strip Records.xlsm')
            , ('Deep strip', 'Deep strip Record')
            , ('TFB', 'TFB records.xlsx')
            , ('TFB', 'TFB')
            , ('Wame record', null)
            , ('Lucid Plain', null)
            , ('Frequency', 'Frequency.xlsx')
            , ('Extra sound', 'Extra Sound.xlsx')
            , ('Extra sound', 'Extra Sound Record')
            , ('Tipsy', null)
            , ('Million records', 'Million Records.xlsx')
            , ('Mark Music Records', 'Mark Music Records.xlsm')
            , ('Pofiqist', 'Pofiqist.xlsx')
            , ('Lopills', null)
            , ('PVMM', 'PVMM.xlsx')
            , ('PVMM', 'PVMM')
            , ('Lofi Jazz Records', null)
            , ('Street Phonk'' Records', 'NHẠC DỪNG SỬ DỤNG.xlsx')
            , ('Street Phonk'' Records', 'NHẠC DỪNG SỬ DỤNG')
            , ('Aurorian '' Records', 'AURORIAN'' RECORD.xlsm')
            , ('Aurorian '' Records', 'AURORIAN'' RECORD')
            , ('HALIDON MUSIC', 'HalidonMusic.xlsx')
            , ('ALEX INSOURATSELOU', 'Thu mua nền tảng.xlsx')
    ) as mapping(partner_name, kho_name)
),

repository_partner_map as (
    select distinct
        partner_name
        , nullif(trim(kho_name), '') as kho_name
    from raw_repository_partner_map
    where nullif(trim(kho_name), '') is not null

    union

    select distinct
        partner_name
        , nullif(trim(regexp_replace(kho_name, '\.(xlsx|xlsm)$', '', 'i')), '') as kho_name
    from raw_repository_partner_map
    where nullif(trim(kho_name), '') is not null
),

odoo_resources as (
    select
        'odoo' as resource_source
        , nullif(trim(cast(id as text)), '') as source_resource_id
        , nullif(trim(cast(id as text)), '') as odoo_id
        , nullif(trim(cast(name as text)), '') as song_code
        , nullif(trim(cast(song_name as text)), '') as resource_name
        , nullif(trim(cast(subgenre_id as text)), '') as repository_id
        , 'Kho sản xuất' as repository_type
        , cast(nullif(trim(cast(review_score as text)), '') as numeric(18,2)) as acceptance_score
        , cast(
            case
                when raw_acceptance_cost > 1000 then raw_acceptance_cost / 25000
                else raw_acceptance_cost
            end as numeric(18,2)
          ) as acceptance_cost
        , cast(nullif(trim(cast(review_date as text)), '') as timestamp) as acceptance_date
        , nullif(trim(cast(detail_line_id as text)), '') as production_plan_detail_id
        , case
            when state = 'approved' and produce_state = 6 then 'Không nghiệm thu'
            when state = 'approved' and (produce_state <> 6 or produce_state is null) then 'Đã nghiệm thu'
            when state = 'draft' then 'Đang sản xuất'
            when state = 'rejected' then 'Không nghiệm thu'
            else null
          end as status
        , nullif(trim(cast(sale_order_id as text)), '') as so_id
        , nullif(trim(cast(purchase_order_line_id as text)), '') as po_detail_id
        , nullif(trim(cast(hg_code as text)), '') as hg_stock_id
    from (
        select
            xms.*
            , cast(nullif(trim(cast(review_price as text)), '') as numeric(18,2)) as raw_acceptance_cost
        from {{ source('staging', 'x_music_song') }} xms
        where active = true
    ) odoo
),

purchased_base as (
    select distinct on (pr."Mã")
        'purchased' as resource_source
        , nullif(trim(pr."Mã"), '') as source_resource_id
        , cast(null as text) as odoo_id
        , cast(null as text) as song_code
        , nullif(trim(pr."Tiêu đề"), '') as resource_name
        , nullif(trim(cast(coalesce(dr_by_kho.repository_id, dr_direct.repository_id, dr_by_partner.repository_id) as text)), '') as repository_id
        , nullif(trim(coalesce(p_direct."Cách tính giá", p_by_kho."Cách tính giá", p_by_partner."Cách tính giá")), '') as repository_type
        , cast(null as numeric(18,2)) as acceptance_score
        , cast(null as numeric(18,2)) as acceptance_cost
        , cast(null as timestamp) as acceptance_date
        , cast(null as text) as production_plan_detail_id
        , 'Đã nghiệm thu' as status
        , cast(null as text) as so_id
        , cast(null as text) as po_detail_id
        , nullif(trim(pr."Mã"), '') as hg_stock_id
    from {{ source('staging', 'purchased_resource') }} pr
    left join repository_partner_map rpm_by_kho
        on lower(regexp_replace(normalize(nullif(trim(pr."Repository"), ''), nfc), '\s+', ' ', 'g')) = lower(regexp_replace(normalize(nullif(trim(rpm_by_kho.kho_name), ''), nfc), '\s+', ' ', 'g'))
    left join repository_partner_map rpm_by_partner
        on lower(regexp_replace(normalize(nullif(trim(pr."Repository"), ''), nfc), '\s+', ' ', 'g')) = lower(regexp_replace(normalize(nullif(trim(rpm_by_partner.partner_name), ''), nfc), '\s+', ' ', 'g'))
    left join {{ ref('dim_repository') }} dr_by_kho
        on lower(regexp_replace(normalize(nullif(trim(rpm_by_kho.partner_name), ''), nfc), '\s+', ' ', 'g')) = lower(regexp_replace(normalize(nullif(trim(dr_by_kho.repository_name), ''), nfc), '\s+', ' ', 'g'))
    left join {{ ref('dim_repository') }} dr_direct
        on lower(regexp_replace(normalize(nullif(trim(pr."Repository"), ''), nfc), '\s+', ' ', 'g')) = lower(regexp_replace(normalize(nullif(trim(dr_direct.repository_name), ''), nfc), '\s+', ' ', 'g'))
    left join {{ ref('dim_repository') }} dr_by_partner
        on lower(regexp_replace(normalize(nullif(trim(rpm_by_partner.kho_name), ''), nfc), '\s+', ' ', 'g')) = lower(regexp_replace(normalize(nullif(trim(dr_by_partner.repository_name), ''), nfc), '\s+', ' ', 'g'))
    left join {{ source('staging', 'partners') }} p_direct
        on lower(regexp_replace(normalize(nullif(trim(pr."Repository"), ''), nfc), '\s+', ' ', 'g')) = lower(regexp_replace(normalize(nullif(trim(p_direct."Tên kho trên HG Stock"), ''), nfc), '\s+', ' ', 'g'))
    left join {{ source('staging', 'partners') }} p_by_kho
        on lower(regexp_replace(normalize(nullif(trim(rpm_by_kho.partner_name), ''), nfc), '\s+', ' ', 'g')) = lower(regexp_replace(normalize(nullif(trim(p_by_kho."Tên kho trên HG Stock"), ''), nfc), '\s+', ' ', 'g'))
    left join {{ source('staging', 'partners') }} p_by_partner
        on lower(regexp_replace(normalize(nullif(trim(rpm_by_partner.kho_name), ''), nfc), '\s+', ' ', 'g')) = lower(regexp_replace(normalize(nullif(trim(p_by_partner."Tên kho trên HG Stock"), ''), nfc), '\s+', ' ', 'g'))
    where trim(pr."Mã") ~ '^HGFA[A-F0-9]+$'
    order by
        pr."Mã"
        , coalesce(dr_by_kho.repository_id, dr_direct.repository_id, dr_by_partner.repository_id) nulls last
        , coalesce(rpm_by_kho.partner_name, pr."Repository", rpm_by_partner.kho_name) nulls last
),

before_odoo_base as (
    select
        rbo.*
        , cast(nullif(trim(rbo."Chi phí đv: $"), '') as numeric(18,2)) as raw_acceptance_cost
        , cast(nullif(trim(rbo."Ngày nghiệm thu"), '') as timestamp) as parsed_acceptance_date
    from {{ source('staging', 'resource_before_odoo') }} rbo
),

before_odoo_resources as (
    select distinct on (trim(rbo."ISRC"))
        'before_odoo' as resource_source
        , nullif(trim(rbo."ISRC"), '') as source_resource_id
        , cast(null as text) as odoo_id
        , nullif(trim(rbo."Mã bài"), '') as song_code
        , nullif(trim(rbo."Tên bài gốc"), '') as resource_name
        , nullif(trim(cast(coalesce(dr.repository_id, dr_no_repo.repository_id) as text)), '') as repository_id
        , 'Kho sản xuất' as repository_type
        , cast(nullif(trim(rbo."Điểm trung bình"), '') as numeric(18,2)) as acceptance_score
        , cast(
            case
                when rbo.raw_acceptance_cost > 1000 then rbo.raw_acceptance_cost / 25000
                else rbo.raw_acceptance_cost
            end as numeric(18,2)
          ) as acceptance_cost
        , rbo.parsed_acceptance_date as acceptance_date
        , cast(null as text) as production_plan_detail_id
        , nullif(trim(rbo."Tình trạng nghiệm thu"), '') as status
        , cast(null as text) as so_id
        , cast(null as text) as po_detail_id
        , nullif(trim(rbo."HG_Stock_ID"), '') as hg_stock_id
    from before_odoo_base rbo
    left join {{ ref('dim_repository') }} dr
        on lower(regexp_replace(normalize(nullif(trim(rbo."Subgenre"), ''), nfc), '\s+', ' ', 'g')) = lower(regexp_replace(normalize(nullif(trim(dr.repository_name), ''), nfc), '\s+', ' ', 'g'))
    left join {{ ref('dim_sub_project') }} dsp
        on lower(regexp_replace(normalize(nullif(trim(rbo."Subgenre"), ''), nfc), '\s+', ' ', 'g')) = lower(regexp_replace(normalize(nullif(trim(dsp.sub_project_name), ''), nfc), '\s+', ' ', 'g'))
    left join {{ ref('dim_repository') }} dr_no_repo
        on lower(regexp_replace(normalize(nullif(trim(dr_no_repo.repository_name), ''), nfc), '\s+', ' ', 'g')) = lower(regexp_replace(normalize('Không có kho', nfc), '\s+', ' ', 'g'))
        and nullif(trim(cast(dr_no_repo.sub_project_id as text)), '') = nullif(trim(cast(dsp.sub_project_id as text)), '')
    where nullif(trim(rbo."ISRC"), '') is not null
        and trim(rbo."ISRC") not in ('#N/A', '#REF!')
        and nullif(trim(rbo."HG_Stock_ID"), '') is not null
        and trim(rbo."HG_Stock_ID") <> 'Không tìm thấy'
        and rbo.parsed_acceptance_date <= cast('2025-09-30' as timestamp)
    order by trim(rbo."ISRC")
),

performance_resources as (
    select distinct on (nullif(trim("Mã Stock"), ''))
        'performance' as resource_source
        , coalesce(
            nullif(trim("ISRC chốt"), ''),
            nullif(trim("ISRC (Stock cũ)"), ''),
            nullif(trim("ISRC (Stock mới)"), '')
          ) as source_resource_id
        , cast(null as text) as odoo_id
        , cast(null as text) as song_code
        , nullif(trim("Tên bài"), '') as resource_name
        , dr.repository_id
        , cast(null as text) as repository_type
        , case 
            when nullif(trim("Điểm nghiệm thu"), '') ~ '^[0-9]+(\.[0-9]+)?$'
            then cast("Điểm nghiệm thu" as numeric(18,2))
            else null
        end as acceptance_score
        , case
            when nullif(regexp_replace("Chi phí sx", '[^0-9.]', '', 'g'), '') ~ '^[0-9]+(\.[0-9]+)?$'
            then cast(regexp_replace("Chi phí sx", '[^0-9.]', '', 'g') as numeric(18,2))
            else null
        end as acceptance_cost
        , cast(nullif(trim("Ngày nghiệm thu"), '') as timestamp) as acceptance_date
        , cast(null as text) as production_plan_detail_id
        , cast(null as text) as status
        , cast(null as text) as so_id
        , cast(null as text) as po_detail_id
        , nullif(trim("Mã Stock"), '') as hg_stock_id
    from {{ source('staging', 'resource_performance') }} rp
    left join {{ ref('dim_repository') }} dr
        on lower(regexp_replace(normalize(nullif(trim(rp."Kho (nếu có)"), ''), nfc), '\s+', ' ', 'g'))
         = lower(regexp_replace(normalize(nullif(trim(dr.repository_name), ''), nfc), '\s+', ' ', 'g'))
    where nullif(trim("Mã Stock"), '') is not null
        and trim("Mã Stock") ~ '^HGFA[A-F0-9]+$'
        and nullif(trim("Ưu tiên"), '') is not null
        and nullif(trim("Mã Stock"), '') not in (
            select hg_stock_id from odoo_resources where hg_stock_id is not null
            union
            select hg_stock_id from purchased_base where hg_stock_id is not null
            union
            select hg_stock_id from before_odoo_resources where hg_stock_id is not null
        )
    order by nullif(trim("Mã Stock"), '')
),

unioned as (
    select * from odoo_resources
    union all
    select * from purchased_base
    union all
    select * from before_odoo_resources
    union all
    select * from performance_resources
)

select
    {{ dbt_utils.generate_surrogate_key(['resource_source', 'source_resource_id']) }} as dim_resources_sk
    , resource_source
    , odoo_id
    , song_code
    , resource_name
    , repository_id
    , repository_type
    , acceptance_score
    , acceptance_cost
    , acceptance_date
    , production_plan_detail_id
    , status
    , so_id
    , po_detail_id
    , hg_stock_id
from unioned
