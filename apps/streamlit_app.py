#!/usr/bin/env python3
"""
Streamlit head-to-head comparison app for ATP Tour data.

Usage:
    uv run streamlit run apps/streamlit_app.py
"""
__author__ = "achilala"
__version__ = "0.1.0"

import logging

import duckdb
import streamlit as st
from streamlit_searchbox import st_searchbox

log = logging.getLogger(__name__)

_db = duckdb.connect("analytics.duckdb", read_only=True)


def search_players(search_term: str) -> list[str]:
    rows = _db.execute(
        """
        select player_name
          from mart.dim_player
         where player_name ilike '%' || $search_term || '%'
         order by player_name
         limit 7
        """,
        {"search_term": search_term},
    ).fetchall()
    return [r[0] for r in rows]


def get_player_info(player_name: str) -> dict:
    return (
        _db.execute(
            """
            select age_incl_date_of_birth  as "Age"
                  ,country_name || ' ' || flag as "Birthplace"
                  ,nationality               as "Nationality"
                  ,continent                 as "Continent"
                  ,height                    as "Height"
                  ,dominant_hand             as "Plays"
                  ,career_wins_vs_losses     as "Career Wins/Losses"
                  ,career_win_ratio          as "Career Win Ratio"
              from mart.dim_player
             where player_name = $player_name
            """,
            {"player_name": player_name},
        )
        .df()
        .transpose()
        .to_dict()
    )


def get_match_history(player1: str, player2: str):
    return _db.execute(
        """
        select "Date"
              ,"Tournament"
              ,"Surface"
              ,"Round"
              ,"Winner"
              ,"Loser"
              ,"Score"
              ,"Matches"
              ,"Aces"
              ,"Year"
              ,"Hand"
          from reporting.rpt_match_summary
         where ("Winner" = $p1 and "Loser" = $p2)
            or ("Winner" = $p2 and "Loser" = $p1)
         order by "Date" desc
        """,
        {"p1": player1, "p2": player2},
    ).df()


def main() -> None:
    st.title("ATP Head to Head")

    left, right = st.columns(2)
    with left:
        player1 = st_searchbox(search_players, label="Player 1", key="p1")
    with right:
        player2 = st_searchbox(search_players, label="Player 2", key="p2")

    st.markdown("---")

    if not (player1 and player2):
        return

    matches = get_match_history(player1, player2)

    if matches.empty:
        st.header(f"{player1} vs {player2}")
        st.error("No matches found between these players.")
        return

    p1_wins = matches[matches["Winner"] == player1].shape[0]
    p2_wins = matches[matches["Winner"] == player2].shape[0]
    st.header(f"{player1} {p1_wins}–{p2_wins} {player2}")

    left, right = st.columns(2)
    with left:
        st.dataframe(get_player_info(player1))
    with right:
        st.dataframe(get_player_info(player2))

    left, right = st.columns(2)
    with left:
        st.markdown("#### By Surface")
        by_surface = _db.sql(
            "select Winner as Player, Surface, count(*) as Wins from matches group by all"
        ).df()
        st.dataframe(by_surface.pivot(index="Surface", columns="Player", values="Wins"))
    with right:
        st.markdown("#### By Round")
        by_round = _db.sql(
            "select Winner as Player, Round, count(*) as Wins from matches group by all"
        ).df()
        st.dataframe(by_round.pivot(index="Round", columns="Player", values="Wins"))

    st.markdown("#### Matches")
    st.dataframe(matches)

    left, right = st.columns(2)
    with left:
        st.markdown("#### Matches over time")
        by_year = _db.sql(
            "select Winner as Player, Year, sum(Matches) as Matches from matches group by all"
        ).df()
        st.line_chart(data=by_year, x="Year", y="Matches")
    with right:
        st.markdown("#### Aces over time")
        by_aces = _db.sql(
            "select Winner as Player, Year, sum(Aces) as Aces from matches group by all"
        ).df()
        st.line_chart(data=by_aces, x="Year", y="Aces")


if __name__ == "__main__":
    main()
