{{ config(tags=["staging"]) }}

with source as (
    select *
      from {{ source('atp_tour', 'players') }}
)
, deduped as (
    -- Temporal patch for duplicate player IDs 148670 / 148671 in the source
    select *
          ,row_number() over (partition by player_id order by player_id) as _row_num
      from source
)
, renamed as (
    select player_id::int                                              as player_id
          ,(name_first || ' ' || name_last)::varchar(100)             as player_name
          ,name_first::varchar(50)                                     as first_name
          ,name_last::varchar(50)                                      as last_name
          ,case
               when hand = 'R' then 'Right-handed'
               when hand = 'L' then 'Left-handed'
               when hand = 'A' then 'Ambidextrous'
               when hand = 'U' then 'Unknown'
               else hand
           end::varchar(15)                                            as dominant_hand
          ,dob::date                                                   as date_of_birth
          ,{{ to_age('dob') }}::smallint                              as age
          ,ioc::varchar(3)                                             as country_iso_code
          ,height::smallint                                            as height_in_centimeters
          ,round(height * {{ var('cm_to_inches') }}, 1)::decimal(4,1) as height_in_inches
          ,wikidata_id::varchar(10)                                    as wikidata_id
          ,1                                                           as num_of_players
      from deduped
     where _row_num = 1
)
, display_cols as (
    select player_id
          ,player_name
          ,first_name
          ,last_name
          ,dominant_hand
          ,{{ to_iso_date('date_of_birth') }}                                                   as date_of_birth
          ,age
          ,(age || ' (' || {{ to_iso_date_us('date_of_birth') }} || ')')::varchar(30)           as age_incl_date_of_birth
          ,country_iso_code
          ,height_in_centimeters
          ,height_in_inches
          ,(replace(height_in_inches::varchar, '.', '''') || '" (' || height_in_centimeters || ' cm)')::varchar(20) as height
          ,wikidata_id
          ,num_of_players
          ,{{ to_date_key('date_of_birth') }}::int                                              as date_of_birth_key
          ,{{ to_time_key('date_of_birth') }}::int                                              as time_of_birth_key
      from renamed
)
select {{ dbt_utils.generate_surrogate_key(['player_id']) }} as player_sk
      ,*
  from display_cols
