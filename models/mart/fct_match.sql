{{ config(
    tags=["mart", "fact"]
  )
}}

with matches as (
	select *
	  from {{ ref('stg_atp_tour__matches') }}
)
, match as (
	select match_sk as dim_match_key
		  ,tournament_sk as dim_tournament_key
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
		  ,winner_ht
		  ,winner_age
		  ,winner_ace
		  ,winner_df
		  ,winner_svpt
		  ,winner_1stin
		  ,winner_1stwon
		  ,winner_2ndwon
		  ,winner_svgms
		  ,winner_bpsaved
		  ,winner_bpfaced
		  ,winner_rank
		  ,winner_rank_points
		  ,loser_seed
		  ,loser_entry
		  ,loser_ht
		  ,loser_age
		  ,loser_ace
		  ,loser_df
		  ,loser_svpt
		  ,loser_1stin
		  ,loser_1stwon
		  ,loser_2ndwon
		  ,loser_svgms
		  ,loser_bpsaved
		  ,loser_bpfaced
		  ,loser_rank
		  ,loser_rank_points
	  from matches
)
select *
  from match