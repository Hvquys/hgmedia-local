{{ config(materialized='ephemeral') }}

/*
Tên bên trái là tên chuẩn dùng chung cho purchased_resource và purchase_cost.
Không đưa các alias nhập nhằng như "Thu mua đối tác" vào mapping.
*/
with raw_map as (
    select *
    from (
        values
            ('Deep Disco', array['Deep Disco', 'Blink - BD - Deep House (Deep Disco)']::text[]),
            ('Das Ohr Digital', array['Das Ohr Digital', 'DAS OHR DIGITAL', 'TECH HOUSE']::text[]),
            ('Martynas Laurinavicius Whitesand', array['Martynas Laurinavicius Whitesand', 'Martynas Laurinavicius (Whitesand)', 'Whitesand']::text[]),
            ('Ardie Son', array['Ardie Son', 'Chung (MKT3 cũ)']::text[]),
            ('PUEBLO VISTA POLYMESA KAI MOUSIKI LP', array['PUEBLO VISTA POLYMESA KAI MOUSIKI LP', 'Pueblo']::text[]),
            ('Selah Instrumental Music', array['Selah Instrumental Music']::text[]),
            ('Breath Of Heaven', array['Breath Of Heaven']::text[]),
            ('Sergei Grishuk (Сергей Грищук) Relax Nga', array['Sergei Grishuk (Сергей Грищук) Relax Nga', 'Relax Nga - MKT1']::text[]),
            ('Oleg Gorbonosov', array['Oleg Gorbonosov']::text[]),
            ('Melnyk Ihor Olegovich', array['Melnyk Ihor Olegovich']::text[]),
            ('Tymur Khakimov', array['Tymur Khakimov']::text[]),
            ('Palash Sunvaiya', array['Palash Sunvaiya']::text[]),
            ('Nikolay Statilko', array['Nikolay Statilko']::text[]),
            ('Orangery', array['Orangery']::text[]),
            ('Aron van Selm', array['Aron van Selm']::text[]),
            ('Sensoria', array['Sensoria']::text[]),
            ('Maniana Records', array['Maniana Records', 'Maniana']::text[]),
            ('Different Twins', array['Different Twins']::text[]),
            ('Road Story Records', array['Road Story Records']::text[]),
            ('Spectrum Recordings', array['Spectrum Recordings']::text[]),
            ('Three Dot House', array['Three Dot House']::text[]),
            ('Zero claim records', array['Zero claim records']::text[]),
            ('Inside records', array['Inside records', 'Inside Record', 'Beyond music']::text[]),
            ('Natural Deep (RA music)', array['Natural Deep (RA music)', 'Natural Deep (RA Music)']::text[]),
            ('Day dose of house', array['Day dose of house']::text[]),
            ('Deep strip', array['Deep strip', 'Deep strip Record', 'Deep Strip Records']::text[]),
            ('TFB records', array['TFB records', 'TFB']::text[]),
            ('Wame record', array['Wame record']::text[]),
            ('Lucid Plain', array['Lucid Plain']::text[]),
            ('Frequency', array['Frequency']::text[]),
            ('Extra Sound Records', array['Extra Sound Records', 'Extra Sound Record', 'Extra sound']::text[]),
            ('Tipsy', array['Tipsy']::text[]),
            ('Million Records', array['Million Records', 'Million records']::text[]),
            ('Mark Music Records', array['Mark Music Records']::text[]),
            ('Pofiqist', array['Pofiqist']::text[]),
            ('Lopills', array['Lopills']::text[]),
            ('PVMM', array['PVMM']::text[]),
            ('Lofi Jazz Records', array['Lofi Jazz Records']::text[]),
            ('Street Phonk'' Records', array['Street Phonk'' Records']::text[]),
            ('Aurorian '' Records', array['Aurorian '' Records', 'AURORIAN'' RECORD', 'AURORIAN'' RECORDS']::text[]),
            ('HalidonMusic', array['HalidonMusic', 'HALIDON MUSIC', 'Halidon Music']::text[]),
            ('ALEX INSOURATSELOU', array['ALEX INSOURATSELOU', 'Thu mua nền tảng']::text[])
    ) as t(canonical_name, aliases)
)

select distinct
    canonical_name
    , lower(
        regexp_replace(
            trim(alias_name)
            , '[[:space:]]+'
            , ' '
            , 'g'
        )
    ) as alias_key
from raw_map
cross join lateral unnest(aliases) as a(alias_name)

