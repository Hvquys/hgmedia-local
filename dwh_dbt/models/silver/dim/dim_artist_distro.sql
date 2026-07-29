select
    {{ dbt_utils.generate_surrogate_key(['"idArtist"']) }} as dim_artist_distro_sk
    , nullif(trim("idArtist"),'') as artist_id
    , nullif(trim("mainArtist"),'') as artist_name
from {{ source('staging','stream_distro') }}
where nullif(trim("idArtist"),'') is not null
group by "idArtist", "mainArtist"