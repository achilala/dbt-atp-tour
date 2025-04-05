#!/usr/bin/env python3
"""
This script runs the Streamlit app for ATP Tourreturn¢œÇ!z`.
"""
__author__ = "achilala"
__version__ = "0.0.1"

import logging
import duckdb
import streamlit as st
from streamlit_searchbox import st_searchbox

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

atp_tour = duckdb.connect("atp_tour.duckdb", read_only=True)

def search_players(search_term: str):
    results = atp_tour.execute(
        """
        select player_name
          from mart.dim_player
         where player_name ilike '%' || $search_term || '%'
         order by player_name
         limit 7
        """,
        {"search_term": search_term}
    ).fetchall()
    return [result[0] for result in results]

def get_player_info(player_name: str) -> dict:
    return atp_tour.execute(
            """
            select p.age_incl_date_of_birth as "Age"
                  ,p.country_name || ' ' || p.flag as "Birthplace"
                  ,p.nationality as "Nationality"
                  ,p.continent as "Continent"
                  ,p.height as "Height"
                  ,p.dominant_hand as "Plays"
                  ,p.career_wins_vs_losses as "Career Wins/Losses"
                  ,p.career_win_ratio as "Career Win Ratio"
              from mart.dim_player p
             where player_name = $player_name
            """,
            {"player_name": player_name}
        ).df().transpose().to_dict()

def get_match_info(player1_name: str, player2_name: str) -> dict:
    return atp_tour.execute(
            """
            select date as "Date"
                  ,tournament as "Tournament"
                  ,surface as "Surface"
                  ,round as "Round"
                  ,winner as "Winner"
                  ,score as "Score"
                  ,matches as "Matches"
                  ,aces as "Aces"
                  ,year as "Year"
                  ,hand as "Hand"
              from mart.rpt_match_summary m
             where (winner = $player1_name and loser = $player2_name) or (loser = $player1_name and winner = $player2_name)
             order by date desc
            """,
            {
                "player1_name": player1_name, 
                "player2_name": player2_name
            }
        ).df()

def main() -> None:
    st.title("ATP Head to Head")

    left, right = st.columns(2)
    with left:
        player1 = st_searchbox(search_players, label="Player 1", key="player1_search")
    with right:
        player2 = st_searchbox(search_players, label="Player 2", key="player2_search")

    st.markdown("***")

    if player1 and player2:
        matches_info = get_match_info(
            player1,
            player2
        )
        
        if len(matches_info) == 0:
            st.header(f"{player1} vs {player2}")
            st.error("No matches found between these players.")
        else:
            player1_wins = matches_info[matches_info.Winner == player1].shape[0]
            player2_wins = matches_info[matches_info.Winner == player2].shape[0]
            st.header(f"{player1} {player1_wins}-{player2_wins} {player2}")
            
            left, right = st.columns(2)
            with left:
                st.dataframe(get_player_info(player1))
            with right:
                st.dataframe(get_player_info(player2))
            
            left, right = st.columns(2)
            with left:
                st.markdown(f'#### By Surface')
                by_surface = atp_tour.sql(
                    """
                    select Winner as Player
                          ,Surface
                          ,count(*) as Wins
                      from matches_info
                     group by all
                    """
                ).df()
                st.dataframe(by_surface.pivot(index="Surface", columns="Player" ,values="Wins"))
                
            with right:
                st.markdown(f'#### By Round')
                by_surface = atp_tour.sql(
                    """
                    select Winner as Player
                          ,Round
                          ,count(*) as Wins
                      from matches_info
                     group by all
                    """
                ).df()
                st.dataframe(by_surface.pivot(index="Round", columns="Player" ,values="Wins"))

            st.markdown(f'#### Matches')
            st.dataframe(matches_info)

            left, right = st.columns(2)
            with left:
                st.markdown(f'#### By Matches')
                by_matches = atp_tour.sql(
                    """
                    select Winner as Player
                          ,Year
                          ,sum(Matches) as Matches
                      from matches_info
                     group by all
                    """
                ).df()
                st.line_chart(data=by_matches, x="Year", y=["Matches"])
                
            with right:
                st.markdown(f'#### By Aces')
                by_aces = atp_tour.sql(
                    """
                    select Winner as Player
                          ,Year
                          ,sum(Aces) as Aces
                      from matches_info
                     group by all
                    """
                ).df()
                st.line_chart(data=by_aces, x="Year", y=["Aces"])

if __name__ == "__main__":
    main()