{{ config(tags=["intermediate"]) }}

with players as (
    select *
      from {{ ref('stg_atp_tour__players') }}
)
, matches as (
    select *
      from {{ ref('stg_atp_tour__matches') }}
)
-- Collect all names used for each player across match records
, match_player_names as (
    select distinct winner_id                as player_id
                   ,winner_name              as player_name
                   ,length(winner_name)      as player_name_length
      from matches
    union
    select distinct loser_id
                   ,loser_name
                   ,length(loser_name)
      from matches
)
, player_names as (
    select player_id
          ,player_name
          ,length(player_name) as player_name_length
      from players
)
-- Prefer whichever name is longer — match records sometimes contain fuller names
, canonical_name as (
    select m.player_id
          ,case
               when m.player_name_length > p.player_name_length then m.player_name
               else p.player_name
           end as player_name
          ,case
               when m.player_name_length < p.player_name_length then m.player_name
           end as player_aka
      from match_player_names m
      join player_names p
        on p.player_id = m.player_id
       and m.player_name != p.player_name
)
, wins as (
    select p.player_id
          ,count(*) as num_of_wins
      from players p
      join matches m on p.player_id = m.winner_id
     group by p.player_id
)
, losses as (
    select p.player_id
          ,count(*) as num_of_losses
      from players p
      join matches m on p.player_id = m.loser_id
     group by p.player_id
)
select p.player_id
      ,coalesce(n.player_name, p.player_name)                                        as player_name
      ,n.player_aka
      ,coalesce(w.num_of_wins, 0)                                                    as num_of_wins
      ,coalesce(l.num_of_losses, 0)                                                  as num_of_losses
      ,(coalesce(w.num_of_wins, 0) || '/' || coalesce(l.num_of_losses, 0))::varchar  as career_wins_vs_losses
      ,round(
           (1.0 * coalesce(w.num_of_wins, 0))
           / nullif(coalesce(w.num_of_wins, 0) + coalesce(l.num_of_losses, 0), 0)
           , 2
       )::varchar                                                                     as career_win_ratio
  from players p
  left join canonical_name n  on p.player_id = n.player_id
  left join wins w             on p.player_id = w.player_id
  left join losses l           on p.player_id = l.player_id
