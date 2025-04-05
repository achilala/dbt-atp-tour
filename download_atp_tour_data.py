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

def create_schema_if_not_exists(schema_name: str) -> None:
    atp_tour.execute(
        f"""
        create schema if not exists {schema_name}
        """
    )

def read_atp_players_csv_file(base_url: str, schema_name: str) -> None:
    atp_players_csv_file = f"{base_url}/atp_players.csv?raw=true"
    # display files to download
    log.debug(colored(f"downloading atp players file: {atp_players_csv_file}", 'blue'))
    # make sure schema exists
    create_schema_if_not_exists(schema_name=schema_name)
    # import players data
    atp_tour.execute(
        f"""
        create or replace table {schema_name}.players as
        select *
          from read_csv_auto($atp_players_csv_file)
        """,
        {
            "atp_players_csv_file": atp_players_csv_file
        }
    )
    # tidying-up strings that cannot be converted to dates
    # i.e 19000000 there's neither a month of 00 nor a date of 00 so defaulting to 1st of Jan
    atp_tour.execute(
        f"""
        alter table {schema_name}.players
            alter dob set data type date using strptime(regexp_replace(dob::varchar, '0000$', '0101'), '%Y%m%d')::date
        """
    )

def read_atp_matches_csv_files(base_url: str, schema_name: str, year_from: int, year_to: int) -> None:
    """
    read csv data from Github in this format i.e https://github.com/JeffSackmann/tennis_atp/blob/master/atp_matches_1968.csv
    using a list comprehension we can loop through these files
    https://www.w3schools.com/python/python_lists_comprehension.asp
    """
    atp_matches_csv_files =[
        f"{base_url}/atp_matches_{year}.csv?raw=true"
        for year in range(year_from, year_to + 1)
    ]
    # display files to download
    log.debug(colored(f"downloading atp matches files: {atp_matches_csv_files}", 'green'))
    # make sure schema exists
    create_schema_if_not_exists(schema_name=schema_name)
    # import matches data
    atp_tour.execute(
        f"""
        create or replace table {schema_name}.matches as
        select *
          from read_csv_auto($atp_matches_csv_files)
        """,
        {
            "atp_matches_csv_files": atp_matches_csv_files
        }
    )
    atp_tour.execute(
        f"""
        alter table {schema_name}.matches
            alter tourney_date set data type date using cast(strptime(cast(tourney_date as string), '%Y%m%d') as date)
        """
    )

def download_countries_data_to_json_file(json_file: str) -> None:
    url = 'https://restcountries.com/v3.1/all'
    # display files to download
    log.debug(colored(f"downloading countries json file: {url}", 'magenta'))
    response = requests.get(url)
    json_data = response.json()
    # Save the data as a JSON file
    with open(json_file, 'w') as file:
        json.dump(json_data, file, indent=4)

def read_countries_data(schema_name: str, json_file: str) -> None:
    # make sure schema exists
    create_schema_if_not_exists(schema_name=schema_name)
    # import matches into raw schema
    atp_tour.execute(
        f"""
        create or replace table {schema_name}.countries as
        select *
          from read_json_auto($json_file)
        """,
        {
            "json_file": json_file
        }
    )

def main() -> None:
    base_url = "https://github.com/JeffSackmann/tennis_atp/blob/master"
    schema_name = "raw"
    json_file = '.countries.json'
    read_atp_players_csv_file(
        base_url=base_url,
        schema_name=schema_name
    )
    read_atp_matches_csv_files(
        base_url=base_url,
        schema_name=schema_name,
        year_from=1968,
        year_to=2024
    )
    download_countries_data_to_json_file(
        json_file=json_file
    )
    read_countries_data(
        schema_name=schema_name,
        json_file=json_file
    )

if __name__ == "__main__":
    main()