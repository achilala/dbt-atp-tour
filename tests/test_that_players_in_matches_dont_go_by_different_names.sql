with atp_tour_matches as (
    select *
      from {{ source('atp_tour', 'matches') }}
)
, names as (
  select winner_id as player_id
        ,winner_name as player_name
    from atp_tour_matches
  union
  select loser_id as player_id
        ,loser_name as player_name
    from atp_tour_matches

)
, duplicates as (
  select player_id
        ,player_name
    from names
   group by all
  having count(1) > 1
)
select *
  from duplicates
 order by player_id