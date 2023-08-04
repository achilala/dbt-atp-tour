{{ config(
    tags=["staging"]
  )
}}

with atp_tour_players as (
    select *
      from {{ source('atp_tour', 'players') }}
)
, renamed as (
    select player_id::int as player_id
          ,name_first||' '||name_last::varchar(100) as player_name
          ,name_first::varchar(50) as first_name
          ,name_last::varchar(50) as last_name
          ,case
              when hand = 'R' then 'Right-handed'
              when hand = 'L' then 'Left-handed'
              when hand = 'A' then 'Ambidextrous'
              when hand = 'U' then 'Unknown'
              else hand
           end::varchar(15) as dominant_hand
          ,dob::date as date_of_birth
          ,(year(current_date) - year(dob))::smallint as age
          ,(year(current_date) - year(dob))||' ('||strftime(dob, '%Y.%m.%d')||')'::varchar(20) as age_incl_date_of_birth
          ,ioc::varchar(3) as country_iso_code
          ,height::smallint as height_cm
          ,wikidata_id::varchar(10) as wikidata_id
      from atp_tour_players
)
, surrogate_keys as (
    select {{ dbt_utils.surrogate_key(['player_id']) }} as player_sk
          ,strftime(date_of_birth, '%Y%m%d') as dob_date_key
          ,*
      from renamed
)
select *
  from surrogate_keys