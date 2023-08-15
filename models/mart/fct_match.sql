{{ config(
    tags=["mart", "fact"]
  )
}}

with matches as (
	select *
	  from {{ ref('stg_atp_tour__matches') }}
)
, players as (
	select *
	  from {{ ref('stg_atp_tour__players') }}
)
, ref_unknown_record as (
	select *
	  from {{ ref('ref_unknown_value') }}
)
, match as (
	select coalesce(tournament_sk, u.unknown_key) as dim_tournament_key
		  ,coalesce(tournament_date_key, u.unknown_key) as dim_tournament_date_key
		  ,coalesce(player_winner_key, u.unknown_key) as dim_player_winner_key
		  ,coalesce(player_loser_key, u.unknown_key) as dim_player_loser_key
		  ,coalesce(w.date_of_birth_key, u.unknown_key) as dim_date_winner_date_of_birth_key
		  ,coalesce(l.date_of_birth_key, u.unknown_key) as dim_date_loser_date_of_birth_key
		  ,score
		  ,best_of
		  ,best_of_labeled
		  ,round
		  ,minutes
		  ,1 as num_of_matches
		  ,winner_seed
		  ,winner_entry
		  ,winner_height_cm
		  ,winner_age
		  ,winner_num_of_aces
		  ,winner_num_of_double_faults
		  ,winner_num_of_serve_pts
		  ,winner_num_of_1st_serves_made
		  ,winner_num_of_1st_serve_pts_won
		  ,winner_num_of_2nd_serve_pts_won
		  ,winner_num_of_serve_games
		  ,winner_num_of_break_pts_saved
		  ,winner_num_of_break_pts_faced
		  ,winner_rank
		  ,winner_rank_pts
		  ,loser_seed
		  ,loser_entry
		  ,loser_height_cm
		  ,loser_age
		  ,loser_num_of_aces
		  ,loser_num_of_double_faults
		  ,loser_num_of_serve_pts
		  ,loser_num_of_1st_serves_made
		  ,loser_num_of_1st_serve_pts_won
		  ,loser_num_of_2nd_serve_pts_won
		  ,loser_num_of_serve_games
		  ,loser_num_of_break_pts_saved
		  ,loser_num_of_break_pts_faced
		  ,loser_rank
		  ,loser_rank_pts
          ,total_num_of_aces
          ,total_num_of_double_faults
          ,total_num_of_serve_pts
          ,total_num_of_1st_serves_made
          ,total_num_of_1st_serve_pts_won
          ,total_num_of_2nd_serve_pts_won
          ,total_num_of_serve_games
          ,total_num_of_break_pts_saved
          ,total_num_of_break_pts_faced
		  ,age_difference
	  from matches m
	  left join ref_unknown_record u on 1 = 1
	  left join players w on m.winner_id = w.player_id
	  left join players l on m.loser_id = l.player_id
)
select *
  from match