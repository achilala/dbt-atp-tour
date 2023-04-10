#!/usr/bin/env python3
"""
This script reads ATP Tour Tennis Tournament data from Github into a duckdb database for analytics
"""
__author__ = "achilala"
__version__ = "0.0.1"

import logging
from termcolor import colored

import duckdb

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


def main() -> None:
    # read csv data from Github in this format i.e https://github.com/JeffSackmann/tennis_atp/blob/master/atp_matches_1968.csv
    BASE_URL = "https://github.com/JeffSackmann/tennis_atp/blob/master"
    atp_players_csv_file = f"{BASE_URL}/atp_players.csv?raw=true"
    # using a list comprehension we can loop through these files
    # https://www.w3schools.com/python/python_lists_comprehension.asp
    year_from=1968
    year_to=2023
    atp_matches_csv_files =[
        f"{BASE_URL}/atp_matches_{year}.csv?raw=true"
        for year in range(year_from, year_to)
    ]

    # read files from http file system and persist the data with duckdb
    atp_tour = duckdb.connect("atp_tour.duckdb")
    atp_tour.sql("INSTALL httpfs")
    atp_tour.sql("LOAD httpfs")
    
    # just messing around with adding colors to messages in the terminal
    log.debug(colored(f"downloading atp players file: {atp_players_csv_file}", 'blue'))
    log.debug(colored(f"downloading atp matches files: {atp_matches_csv_files}", 'green'))

    # make sure schema for raw data exists
    atp_tour.execute(
        """
        create schema if not exists raw
        """
    )

    # import players into raw schema
    atp_tour.execute(
        """
        create or replace table raw.players as
        select *
          from read_csv_auto(
            $1
        )
        """,
        [atp_players_csv_file]
    )

    # import matches into raw schema
    atp_tour.execute(
        """
        create or replace table raw.matches as
        select *
          from read_csv_auto(
            $1,
            types={
                "winner_seed": varchar,
                "loser_seed": varchar
            }
        )
        """,
        [atp_matches_csv_files]
    )

    # tidying-up strings that cannot be converted to dates
    # i.e 19000000 there's neither a month of 00 nor a date of 00 so defaulting to 1st of Jan
    atp_tour.execute(
        """
        alter table raw.players
            alter dob set data type date using cast(strptime(cast(regexp_replace(dob, '0000$', '0101') as string), '%Y%m%d') as date)
        """
    )
    atp_tour.execute(
        """
        alter table raw.matches
            alter tourney_date set data type date using cast(strptime(cast(tourney_date as string), '%Y%m%d') as date)
        """
    )

if __name__ == "__main__":
    main()