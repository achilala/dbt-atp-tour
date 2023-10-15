with atp_tour_matches as (
    select *
      from {{ source('atp_tour', 'matches') }}
)
, players as (
  select winner_id as player_id
        ,winner_name as player_name
    from atp_tour_matches
  union
  select loser_id as player_id
        ,loser_name as player_name
    from atp_tour_matches

)
, players_going_by_different_names as (
  select player_id
        ,player_name
    from players
   group by all
  having count(1) > 1
)
select *
  from players_going_by_different_names
 order by player_id