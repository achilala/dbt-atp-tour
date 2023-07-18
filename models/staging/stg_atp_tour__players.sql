{{ config(
    tags=["staging"]
  )
}}

with atp_tour_players as (
    select *
      from {{ source('atp_tour', 'players') }}
)
, renamed as (
    select player_id
          ,name_first||' '||name_last as player_name
          ,name_first as first_name
          ,name_last as last_name
          ,case
              when hand = 'R' then 'Right-handed'
              when hand = 'L' then 'Left-handed' 
              else 'Unknown' 
           end as dominant_hand
          ,dob as date_of_birth
          ,year(current_date) - year(dob) as age
          ,ioc as iso_country_code
          ,height as height_cm
          ,wikidata_id
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