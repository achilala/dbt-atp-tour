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
, countries as (
	select *
	  from {{ ref('stg_atp_tour__countries') }}
)
, ref_unknown_record as (
	select *
	  from {{ ref('ref_unknown_value') }}
)
, match_player_names as (
  select distinct winner_id as player_id
        ,winner_name as player_name
        ,length(player_name) as player_name_length
    from matches
  union
  select distinct loser_id as player_id
        ,loser_name as player_name
        ,length(player_name) as player_name_length
    from matches
)
, player_names as (
  select player_id
        ,player_name
        ,length(player_name) as player_name_length
    from players
)
, longer_player_name as (
  select m.player_id
        ,case 
            when m.player_name_length > p.player_name_length then m.player_name 
            else p.player_name
         end as player_name
        ,case 
            when m.player_name_length < p.player_name_length then m.player_name
         end as player_aka
        ,m.player_name as m
        ,p.player_name as p
    from match_player_names m
    join player_names p on p.player_id = m.player_id and m.player_name != p.player_name
)
, num_of_wins_by_player as (
	select p.player_id
        ,count(w.winner_id) as num_of_wins
	  from players p
	  join matches w on p.player_id = w.winner_id
   group by all
)
, num_of_losses_by_player as (
	select p.player_id
        ,count(l.loser_id) as num_of_losses
	  from players p
	  join matches l on p.player_id = l.loser_id
   group by all
)
, unknown_record as (
	select unknown_key as dim_player_key
		    ,unknown_integer as player_id
        ,unknown_text as player_name
        ,unknown_text as player_aka
        ,unknown_text as first_name
        ,unknown_text as last_name
        ,unknown_text as dominant_hand
        -- ,unknown_date as date_of_birth
        ,unknown_text as age_incl_date_of_birth
        ,unknown_integer as age
        ,unknown_text as nationality
        ,unknown_text as country_iso_code
        ,unknown_text as country_name
        ,unknown_text as continent
        ,unknown_integer as height_in_centimeters
        ,unknown_float as height_in_inches
        ,unknown_text as height
        ,unknown_text as wikidata_id
        ,unknown_integer as num_of_wins
        ,unknown_integer as num_of_losses
        ,unknown_text as career_wins_vs_losses
        ,unknown_float as career_win_ratio
    from ref_unknown_record

    union all
    
	select p.player_sk as dim_player_key
		    ,p.player_id
        ,coalesce(n.player_name, p.player_name) as player_name
        ,n.player_aka as player_aka
        ,p.first_name
        ,p.last_name
        ,p.dominant_hand
        -- ,p.date_of_birth
        ,p.age_incl_date_of_birth
        ,p.age
        ,c.nationality
        ,p.country_iso_code
        ,c.country_name
        ,c.continent
        ,p.height_in_centimeters
        ,p.height_in_inches
        ,p.height
        ,p.wikidata_id
        ,w.num_of_wins
        ,l.num_of_losses
        ,w.num_of_wins||'/'||l.num_of_losses as career_wins_vs_losses
        ,round((1.0 * w.num_of_wins) / (w.num_of_wins + l.num_of_losses), 2) as career_win_ratio
	  from players p
	  left join longer_player_name n on p.player_id = n.player_id
	  left join num_of_wins_by_player w on p.player_id = w.player_id
	  left join num_of_losses_by_player l on p.player_id = l.player_id
	  left join countries c on c.country_iso_code = p.country_iso_code
)
select *
  from unknown_record