#!/usr/bin/env python3
"""
This script reads ATP Tour Tennis Tournament data from Github into a DuckDB database for analytics.
"""
__author__ = "achilala"
__version__ = "0.0.4"

import logging
import duckdb
import requests
import json
import re
from termcolor import colored

GITHUB_OWNER = "JeffSackmann"
GITHUB_REPO = "tennis_atp"
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents"
SCHEMA_NAME = "raw"
DB_FILE = "atp_tour.duckdb"
COUNTRIES_JSON_FILE = ".countries.json"
COUNTRIES_API_URL = 'https://restcountries.com/v3.1/all?fields=cca3,name,demonyms,region,subregion,flag,population,borders'

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def get_duckdb_connection(db_file=DB_FILE):
    con = duckdb.connect(db_file)
    con.sql("install httpfs")
    con.sql("load httpfs")
    return con

def create_schema_if_not_exists(con, schema_name: str) -> None:
    con.execute(f"create schema if not exists {schema_name}")

def get_repo_file_urls(file_pattern: str) -> list:
    """
    Fetches files from the GitHub repo matching the provided regex pattern.
    """
    response = requests.get(GITHUB_API_URL)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch files: {response.status_code}")
    
    files = response.json()
    pattern = re.compile(file_pattern)
    return [file['download_url'] for file in files if pattern.match(file['name'])]

def read_atp_players(con, schema_name: str, file_urls: list) -> None:
    log.info(colored(f"Downloading ATP players data from: {file_urls}", 'blue'))
    create_schema_if_not_exists(con, schema_name)
    
    con.execute(
        f"""
        create or replace table {schema_name}.players as
        select * from read_csv_auto($file_urls)
        """,
        {"file_urls": file_urls}
    )
    
    con.execute(
        f"""
        alter table {schema_name}.players
        alter dob set data type date using 
        strptime(regexp_replace(cast(dob as varchar), '0000$', '0101'), '%Y%m%d')::date
        """
    )

def read_atp_matches(con, schema_name: str, file_urls: list) -> None:
    log.info(colored(f"Downloading ATP matches data from: {file_urls}", 'green'))
    create_schema_if_not_exists(con, schema_name)
    
    con.execute(
        f"""
        create or replace table {schema_name}.matches as
        select * from read_csv_auto($file_urls)
        """,
        {"file_urls": file_urls}
    )
    
    con.execute(
        f"""
        alter table {schema_name}.matches
        alter tourney_date set data type date using 
        cast(strptime(cast(tourney_date as varchar), '%Y%m%d') as date)
        """
    )

def download_and_save_countries_json(file_path: str, url: str = COUNTRIES_API_URL) -> None:
    log.info(colored(f"Downloading countries data from: {url}", 'magenta'))
    response = requests.get(url)
    response.raise_for_status()
    
    with open(file_path, 'w') as file:
        json.dump(response.json(), file, indent=4)

def read_countries(con, schema_name: str, file_path: str) -> None:
    create_schema_if_not_exists(con, schema_name)
    
    con.execute(
        f"""
        create or replace table {schema_name}.countries as
        select * from read_json_auto($file_path)
        """,
        {"file_path": file_path}
    )

def main() -> None:
    con = get_duckdb_connection()
    
    # Fetch file URLs first
    players_urls = get_repo_file_urls(r"atp_players\.csv")
    matches_urls = get_repo_file_urls(r"atp_matches_\d{4}\.csv")
    
    if not players_urls:
        raise Exception("ATP players file not found.")
    if not matches_urls:
        raise Exception("No ATP matches files found.")
    
    # Pass file URLs explicitly (dependency injection)
    read_atp_players(con, schema_name=SCHEMA_NAME, file_urls=players_urls)
    read_atp_matches(con, schema_name=SCHEMA_NAME, file_urls=matches_urls)
    
    download_and_save_countries_json(file_path=COUNTRIES_JSON_FILE)
    read_countries(con, schema_name=SCHEMA_NAME, file_path=COUNTRIES_JSON_FILE)
    
    con.close()
    log.info(colored("Data import complete!", 'cyan'))

if __name__ == "__main__":
    main()
