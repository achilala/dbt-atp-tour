{{ config(
    tags=["mart", "dimension"]
  )
}}

with players as (
	select *
	  from {{ ref('stg_atp_tour__players') }}
)
, matches as (
	select *
	  from {{ ref('stg_atp_tour__matches') }}
)
, ref_unknown_record as (
	select *
	  from {{ ref('ref_unknown_value') }}
)
, num_of_wins_by_player as (
	select p.player_id
        ,count(w.winner_id) as num_of_wins
	  from players p
	  join matches w on p.player_id = w.winner_id
   group by p.player_id
)
, num_of_losses_by_player as (
	select p.player_id
        ,count(l.loser_id) as num_of_losses
	  from players p
	  join matches l on p.player_id = l.loser_id
   group by p.player_id
)
, unknown_record as (
	select p.player_sk as dim_player_key
		    ,p.player_id
        ,p.first_name
        ,p.last_name
        ,p.hand
        ,p.dob
        ,p.ioc
        ,p.height
        ,p.wikidata_id
        ,w.num_of_wins
        ,l.num_of_losses
	  from players p
	  left join num_of_wins_by_player w on p.player_id = w.player_id
	  left join num_of_losses_by_player l on p.player_id = l.player_id

    union all
    
	select unknown_key as dim_player_key
		    ,unknown_integer as player_id
        ,unknown_text as first_name
        ,unknown_text as last_name
        ,unknown_text as hand
        ,unknown_date as dob
        ,unknown_text as ioc
        ,unknown_integer as height
        ,unknown_text as wikidata_id
        ,unknown_integer as num_of_wins
        ,unknown_integer as num_of_losses
    from ref_unknown_record
)
select *
  from unknown_record