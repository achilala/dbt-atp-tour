{{ config(tags=["intermediate"]) }}

with matches as (
    select *
      from {{ ref('stg_atp_tour__matches') }}
)
, enriched as (
    select *
          -- Combined match statistics (winner + loser)
          ,(winner_num_of_aces::int           + loser_num_of_aces::int)           as total_num_of_aces
          ,(winner_num_of_double_faults::int  + loser_num_of_double_faults::int)  as total_num_of_double_faults
          ,(winner_num_of_serve_pts::int      + loser_num_of_serve_pts::int)      as total_num_of_serve_pts
          ,(winner_num_of_1st_serves_made::int + loser_num_of_1st_serves_made::int) as total_num_of_1st_serves_made
          ,(winner_num_of_1st_serve_pts_won::int + loser_num_of_1st_serve_pts_won::int) as total_num_of_1st_serve_pts_won
          ,(winner_num_of_2nd_serve_pts_won::int + loser_num_of_2nd_serve_pts_won::int) as total_num_of_2nd_serve_pts_won
          ,(winner_num_of_serve_games::int    + loser_num_of_serve_games::int)    as total_num_of_serve_games
          ,(winner_num_of_break_pts_saved::int + loser_num_of_break_pts_saved::int) as total_num_of_break_pts_saved
          ,(winner_num_of_break_pts_faced::int + loser_num_of_break_pts_faced::int) as total_num_of_break_pts_faced
          -- Derived match attributes
          ,abs(winner_age::int - loser_age::int) as age_difference
      from matches
)
select *
  from enriched
