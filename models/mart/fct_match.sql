{{ config(
    tags=["mart", "fact"]
  )
}}

with matches as (
	select *
	  from {{ ref('stg_atp_tour__matches') }}
)
, match as (
	select tournament_sk as dim_tournament_key
		  ,tournament_date_key as dim_tournament_date_key
		  ,player_winner_key as dim_player_winner_key
		  ,player_loser_key as dim_player_loser_key
		  ,score
		  ,best_of
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
	  from matches
)
select *
  from match