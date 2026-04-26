{{ config(tags=["mart", "fact"]) }}

-- Grain: one row per ATP match.
-- dim_match from v1 has been absorbed here as degenerate dimensions
-- (score, round, best_of, entry types). This avoids an unnecessary join
-- in reporting models and keeps the star schema simpler.

with matches as (
    select *
      from {{ ref('int_atp__matches_enriched') }}
)
, players as (
    select *
      from {{ ref('stg_atp_tour__players') }}
)
, ref_unknown_record as (
    select *
      from {{ ref('ref_unknown_values') }}
)
, fact as (
    select -- Foreign keys
           coalesce(m.tournament_sk,      u.unknown_key::varchar)  as dim_tournament_key
          ,coalesce(m.tournament_date_key, u.unknown_key)::integer  as dim_tournament_date_key
          ,coalesce(m.player_winner_key,  u.unknown_key::varchar)  as dim_player_winner_key
          ,coalesce(m.player_loser_key,   u.unknown_key::varchar)  as dim_player_loser_key
          ,coalesce(w.date_of_birth_key,  u.unknown_key)::integer  as dim_date_winner_dob_key
          ,coalesce(l.date_of_birth_key,  u.unknown_key)::integer  as dim_date_loser_dob_key

          -- Degenerate dimensions (formerly dim_match)
          ,m.match_id
          ,m.score
          ,m.best_of
          ,m.best_of_labeled
          ,m.round
          ,m.winner_entry
          ,m.loser_entry

          -- Measures
          ,1                                                         as num_of_matches
          ,coalesce(m.minutes,                            0)         as minutes
          ,coalesce(m.winner_seed,                        0)         as winner_seed
          ,coalesce(m.winner_height_cm,                   0)         as winner_height_cm
          ,coalesce(m.winner_age,                         0)         as winner_age
          ,coalesce(m.winner_num_of_aces,                 0)         as winner_num_of_aces
          ,coalesce(m.winner_num_of_double_faults,        0)         as winner_num_of_double_faults
          ,coalesce(m.winner_num_of_serve_pts,            0)         as winner_num_of_serve_pts
          ,coalesce(m.winner_num_of_1st_serves_made,      0)         as winner_num_of_1st_serves_made
          ,coalesce(m.winner_num_of_1st_serve_pts_won,    0)         as winner_num_of_1st_serve_pts_won
          ,coalesce(m.winner_num_of_2nd_serve_pts_won,    0)         as winner_num_of_2nd_serve_pts_won
          ,coalesce(m.winner_num_of_serve_games,          0)         as winner_num_of_serve_games
          ,coalesce(m.winner_num_of_break_pts_saved,      0)         as winner_num_of_break_pts_saved
          ,coalesce(m.winner_num_of_break_pts_faced,      0)         as winner_num_of_break_pts_faced
          ,coalesce(m.winner_rank,                        0)         as winner_rank
          ,coalesce(m.winner_rank_pts,                    0)         as winner_rank_pts
          ,coalesce(m.loser_seed,                         0)         as loser_seed
          ,coalesce(m.loser_height_cm,                    0)         as loser_height_cm
          ,coalesce(m.loser_age,                          0)         as loser_age
          ,coalesce(m.loser_num_of_aces,                  0)         as loser_num_of_aces
          ,coalesce(m.loser_num_of_double_faults,         0)         as loser_num_of_double_faults
          ,coalesce(m.loser_num_of_serve_pts,             0)         as loser_num_of_serve_pts
          ,coalesce(m.loser_num_of_1st_serves_made,       0)         as loser_num_of_1st_serves_made
          ,coalesce(m.loser_num_of_1st_serve_pts_won,     0)         as loser_num_of_1st_serve_pts_won
          ,coalesce(m.loser_num_of_2nd_serve_pts_won,     0)         as loser_num_of_2nd_serve_pts_won
          ,coalesce(m.loser_num_of_serve_games,           0)         as loser_num_of_serve_games
          ,coalesce(m.loser_num_of_break_pts_saved,       0)         as loser_num_of_break_pts_saved
          ,coalesce(m.loser_num_of_break_pts_faced,       0)         as loser_num_of_break_pts_faced
          ,coalesce(m.loser_rank,                         0)         as loser_rank
          ,coalesce(m.loser_rank_pts,                     0)         as loser_rank_pts
          ,coalesce(m.total_num_of_aces,                  0)         as total_num_of_aces
          ,coalesce(m.total_num_of_double_faults,         0)         as total_num_of_double_faults
          ,coalesce(m.total_num_of_serve_pts,             0)         as total_num_of_serve_pts
          ,coalesce(m.total_num_of_1st_serves_made,       0)         as total_num_of_1st_serves_made
          ,coalesce(m.total_num_of_1st_serve_pts_won,     0)         as total_num_of_1st_serve_pts_won
          ,coalesce(m.total_num_of_2nd_serve_pts_won,     0)         as total_num_of_2nd_serve_pts_won
          ,coalesce(m.total_num_of_serve_games,           0)         as total_num_of_serve_games
          ,coalesce(m.total_num_of_break_pts_saved,       0)         as total_num_of_break_pts_saved
          ,coalesce(m.total_num_of_break_pts_faced,       0)         as total_num_of_break_pts_faced
          ,coalesce(m.age_difference,                     0)         as age_difference
      from matches m
      left join ref_unknown_record u on 1 = 1
      left join players w on m.winner_id = w.player_id
      left join players l on m.loser_id  = l.player_id
)
select *
  from fact
