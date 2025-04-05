{{ config(
    tags=["mart", "report"]
  )
}}

with matches as (
	select *
	  from {{ ref('fct_match') }}
)
, dates as (
	select *
	  from {{ ref('dim_date') }}
)
, tournaments as (
	select *
	  from {{ ref('dim_tournament') }}
)
, players as (
	select *
	  from {{ ref('dim_player') }}
)
, dim_match as (
	select *
	  from {{ ref('dim_match') }}
)
, renamed as (
  select d.date_day as "Date"
        ,t.tournament_name as "Tournament"
        ,t.surface as "Surface"
        ,dm.round as "Round"
        ,w.player_name as "Winner"
        ,l.player_name as "Loser"
        ,dm.score as "Score"
        ,m.num_of_matches as "Matches"
        ,m.winner_num_of_aces as "Aces"
        ,d.year as "Year"
        ,w.dominant_hand as "Hand"
    from matches m
    join dates d on d.dim_date_key = m.dim_tournament_date_key
    join tournaments t on t.dim_tournament_key = m.dim_tournament_key
    join dim_match dm on dm.dim_match_key = m.dim_match_key
    join players w on w.dim_player_key = m.dim_player_winner_key
    join players l on l.dim_player_key = m.dim_player_loser_key
)
select *
  from renamed