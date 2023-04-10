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
		  ,tourney_sk as dim_tournament_key
		  ,tourney_date_key as dim_tournament_date_key
		  ,winner_player_key as dim_winner_player_key
		  ,loser_player_key as dim_loser_player_key
		  ,score
		  ,best_of
		  ,round
		  ,minutes
		  ,1 as num_of_matches
	  from matches
)
select *
  from match