with names as (
  select winner_id as player_id
        ,winner_name as player_name
    from raw.matches
  union
  select loser_id as player_id
        ,loser_name as player_name
    from raw.matches

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