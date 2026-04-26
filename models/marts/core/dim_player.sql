{{ config(tags=["mart", "dimension"]) }}

with players as (
    select *
      from {{ ref('stg_atp_tour__players') }}
)
, career_stats as (
    select *
      from {{ ref('int_atp__player_career_stats') }}
)
, countries as (
    select *
      from {{ ref('stg_atp_tour__countries') }}
)
, ref_unknown_record as (
    select *
      from {{ ref('ref_unknown_values') }}
)
, unknown_record as (
    select unknown_key::varchar  as dim_player_key
          ,unknown_text::varchar  as player_id
          ,unknown_text::varchar  as player_name
          ,unknown_text::varchar  as player_aka
          ,unknown_text::varchar  as first_name
          ,unknown_text::varchar  as last_name
          ,unknown_text::varchar  as dominant_hand
          ,unknown_text::varchar  as date_of_birth
          ,unknown_text::varchar  as age_incl_date_of_birth
          ,unknown_integer::int   as age
          ,unknown_text::varchar  as nationality
          ,unknown_text::varchar  as country_iso_code
          ,unknown_text::varchar  as country_name
          ,unknown_text::varchar  as flag
          ,unknown_integer::int   as population
          ,unknown_text::varchar  as region
          ,unknown_text::varchar  as continent
          ,unknown_integer::int   as height_in_centimeters
          ,unknown_float::decimal as height_in_inches
          ,unknown_text::varchar  as height
          ,unknown_text::varchar  as wikidata_id
          ,unknown_integer::int   as num_of_players
          ,unknown_integer::int   as num_of_wins
          ,unknown_integer::int   as num_of_losses
          ,unknown_text::varchar  as career_wins_vs_losses
          ,unknown_text::varchar  as career_win_ratio
      from ref_unknown_record

    union all

    select p.player_sk                             as dim_player_key
          ,p.player_id::varchar
          ,cs.player_name
          ,cs.player_aka
          ,p.first_name
          ,p.last_name
          ,p.dominant_hand
          ,p.date_of_birth
          ,p.age_incl_date_of_birth
          ,p.age
          ,c.nationality
          ,c.country_iso_code
          ,c.country_name
          ,c.flag
          ,c.population
          ,c.region
          ,c.continent
          ,p.height_in_centimeters
          ,p.height_in_inches
          ,p.height
          ,p.wikidata_id
          ,p.num_of_players
          ,cs.num_of_wins
          ,cs.num_of_losses
          ,cs.career_wins_vs_losses
          ,cs.career_win_ratio
      from players p
      left join career_stats cs on p.player_id = cs.player_id
      left join countries c     on p.country_iso_code = c.country_iso_code
)
select *
  from unknown_record
