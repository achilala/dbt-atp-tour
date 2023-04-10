{{ config(
    tags=["staging"]
  )
}}

with atp_tour_matches as (
    select *
      from {{ source('atp_tour', 'matches') }}
)
, renamed as (
    select tourney_id
          ,tourney_name
          ,tourney_level
          ,tourney_date
          ,surface
          ,draw_size
          ,match_num as match_id
          ,score
          ,best_of
          ,round
          ,minutes
          ,winner_id
          ,winner_seed
          ,winner_entry
          ,winner_name
          ,winner_hand
          ,winner_ht
          ,winner_ioc
          ,winner_age
          ,w_ace as winner_ace
          ,w_df as winner_df
          ,w_svpt as winner_svpt
          ,w_1stin as winner_1stin
          ,w_1stwon as winner_1stwon
          ,w_2ndwon as winner_2ndwon
          ,w_svgms as winner_svgms
          ,w_bpsaved as winner_bpsaved
          ,w_bpfaced as winner_bpfaced
          ,winner_rank
          ,winner_rank_points
          ,loser_id
          ,loser_seed
          ,loser_entry
          ,loser_name
          ,loser_hand
          ,loser_ht
          ,loser_ioc
          ,loser_age
          ,l_ace as loser_ace
          ,l_df as loser_df
          ,l_svpt as loser_svpt
          ,l_1stin as loser_1stin
          ,l_1stwon as loser_1stwon
          ,l_2ndwon as loser_2ndwon
          ,l_svgms as loser_svgms
          ,l_bpsaved as loser_bpsaved
          ,l_bpfaced as loser_bpfaced
          ,loser_rank
          ,loser_rank_points
      from atp_tour_matches
)
, surrogate_keys as (
    select {{ dbt_utils.surrogate_key(['tourney_id', 'tourney_date']) }} as tourney_sk
          ,{{ dbt_utils.surrogate_key(['tourney_id', 'match_id']) }} as match_sk
          ,strftime(tourney_date, '%Y%m%d') as tourney_date_key
          ,{{ dbt_utils.surrogate_key(['winner_id']) }} as winner_player_key
          ,{{ dbt_utils.surrogate_key(['loser_id']) }} as loser_player_key
          ,*
      from renamed
)
select *
  from surrogate_keys