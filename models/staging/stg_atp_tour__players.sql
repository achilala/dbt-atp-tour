{{ config(
    tags=["staging"]
  )
}}

with atp_tour_players as (
    select *
      from {{ source('atp_tour', 'players') }}
)
, no_duplicate_players as (
     -- temporal patch awaiting permanent fix
    select *
      from atp_tour_players
     where player_id not in (148670, 148671)
)
, conversion_units as (
    select *
      from {{ ref('ref_conversion_units') }}
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
          ,ioc::varchar(3) as country_iso_code
          ,height::smallint as height_in_centimeters
          ,round(height * cu.centimeters_to_inches, 1)::decimal(3,1) as height_in_inches
          ,wikidata_id::varchar(10) as wikidata_id
      from no_duplicate_players p
      left join conversion_units cu on 1 = 1
)
, renamed2 as (
    select player_id
          ,player_name
          ,first_name
          ,last_name
          ,dominant_hand
          ,date_of_birth
          ,age
          ,age||' ('||strftime(date_of_birth, '%Y.%m.%d')||')'::varchar(20) as age_incl_date_of_birth
          ,age_incl_date_of_birth
          ,country_iso_code
          ,height_in_centimeters
          ,height_in_inches
          ,replace(height_in_inches, '.', '''')||'" ('||height_in_centimeters||' cm)'::varchar(20) as height
          ,wikidata_id
      from renamed
)
, surrogate_keys as (
    select {{ dbt_utils.surrogate_key(['player_id']) }} as player_sk
          ,strftime(date_of_birth, '%Y%m%d') as date_of_birth_key
          ,*
      from renamed2
)
select *
  from surrogate_keys