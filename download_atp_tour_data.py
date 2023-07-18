#!/usr/bin/env python3
"""
This script reads ATP Tour Tennis Tournament data from Github into a duckdb database for analytics
"""
__author__ = "achilala"
__version__ = "0.0.1"

import logging
from termcolor import colored # just messing around with adding colors to messages in the terminal
import duckdb
import requests
import json 

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


# read files from http file system and persist the data with duckdb
atp_tour = duckdb.connect("atp_tour.duckdb")
atp_tour.sql("INSTALL httpfs")
atp_tour.sql("LOAD httpfs")

BASE_URL = "https://github.com/JeffSackmann/tennis_atp/blob/master"

def create_schema() -> None:
    atp_tour.execute(
        """
        create schema if not exists raw
        """
    )

def read_atp_players_csv_file() -> None:
    atp_players_csv_file = f"{BASE_URL}/atp_players.csv?raw=true"
    # display files to download
    log.debug(colored(f"downloading atp players file: {atp_players_csv_file}", 'blue'))
    # make sure schema for raw data exists
    create_schema()
    # import players into raw schema
    atp_tour.execute(
        """
        create or replace table raw.players as
        select *
          from read_csv_auto($atp_players_csv_file)
        """,
        {"atp_players_csv_file": atp_players_csv_file}
    )
    # tidying-up strings that cannot be converted to dates
    # i.e 19000000 there's neither a month of 00 nor a date of 00 so defaulting to 1st of Jan
    atp_tour.execute(
        """
        alter table raw.players
            alter dob set data type date using cast(strptime(cast(regexp_replace(dob, '0000$', '0101') as string), '%Y%m%d') as date)
        """
    )

def read_atp_matches_csv_files() -> None:
    # read csv data from Github in this format i.e https://github.com/JeffSackmann/tennis_atp/blob/master/atp_matches_1968.csv
    # using a list comprehension we can loop through these files
    # https://www.w3schools.com/python/python_lists_comprehension.asp
    atp_matches_csv_files =[
        f"{BASE_URL}/atp_matches_{year}.csv?raw=true"
        for year in range(1968, 2023)
    ]
    # display files to download
    log.debug(colored(f"downloading atp matches files: {atp_matches_csv_files}", 'green'))
    # make sure schema for raw data exists
    create_schema();
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
    atp_tour.execute(
        """
        alter table raw.matches
            alter tourney_date set data type date using cast(strptime(cast(tourney_date as string), '%Y%m%d') as date)
        """
    )

def download_and_read_countries_json_file() -> None:
    url = 'https://restcountries.com/v3.1/all'
    # display files to download
    log.debug(colored(f"downloading countries json file: {url}", 'magenta'))
    response = requests.get(url)
    json_data = response.json()
    # Save the data as a JSON file
    json_file = '_countries.json'
    with open(json_file, 'w') as file:
        json.dump(json_data, file, indent=4)

    # import matches into raw schema
    atp_tour.execute(
        """
        create or replace table raw.countries as
        select *
          from read_json_auto($json_file)
        """,
        {"json_file": json_file}
    )

def main() -> None:
    read_atp_players_csv_file()
    read_atp_matches_csv_files()
    download_and_read_countries_json_file()

if __name__ == "__main__":
    main()