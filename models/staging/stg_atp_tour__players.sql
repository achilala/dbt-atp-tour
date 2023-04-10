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
          ,name_first as first_name
          ,name_last as last_name
          ,hand
          ,dob
          ,ioc
          ,height
          ,wikidata_id
      from atp_tour_players
)
, surrogate_keys as (
    select {{ dbt_utils.surrogate_key(['player_id']) }} as player_sk
          ,strftime(dob, '%Y%m%d') as dob_date_key
          ,*
      from renamed
)
select *
  from surrogate_keys