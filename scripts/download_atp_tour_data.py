#!/usr/bin/env python3
"""
Fetches ATP Tour data from GitHub and the REST Countries API into a local DuckDB file.

Usage:
    uv run scripts/download_atp_tour_data.py
"""
__author__ = "achilala"
__version__ = "0.1.0"

import json
import logging
import re

import duckdb
import requests
from termcolor import colored

GITHUB_OWNER = "JeffSackmann"
GITHUB_REPO = "tennis_atp"
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents"
COUNTRIES_API_URL = (
    "https://restcountries.com/v3.1/all"
    "?fields=cca3,name,demonyms,region,subregion,flag,population,borders"
)

SCHEMA_NAME = "raw"
DB_FILE = "atp_tour.duckdb"
COUNTRIES_CACHE = ".countries.json"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


def get_duckdb_connection(db_file: str = DB_FILE) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_file)
    con.sql("install httpfs; load httpfs")
    return con


def create_schema(con: duckdb.DuckDBPyConnection, schema: str) -> None:
    con.execute(f"create schema if not exists {schema}")


def get_repo_file_urls(pattern: str) -> list[str]:
    """Return download URLs for files in the GitHub repo matching the regex pattern."""
    response = requests.get(GITHUB_API_URL)
    response.raise_for_status()
    compiled = re.compile(pattern)
    return [
        f["download_url"]
        for f in response.json()
        if compiled.match(f["name"])
    ]


def load_players(con: duckdb.DuckDBPyConnection, urls: list[str]) -> None:
    log.info(colored(f"Downloading ATP players from {len(urls)} file(s)…", "blue"))
    create_schema(con, SCHEMA_NAME)
    con.execute(
        f"create or replace table {SCHEMA_NAME}.players as select * from read_csv_auto($urls)",
        {"urls": urls},
    )
    con.execute(
        f"""
        alter table {SCHEMA_NAME}.players
        alter dob set data type date using
            strptime(regexp_replace(cast(dob as varchar), '0000$', '0101'), '%Y%m%d')::date
        """
    )


def load_matches(con: duckdb.DuckDBPyConnection, urls: list[str]) -> None:
    log.info(colored(f"Downloading ATP matches from {len(urls)} file(s)…", "green"))
    create_schema(con, SCHEMA_NAME)
    con.execute(
        f"create or replace table {SCHEMA_NAME}.matches as select * from read_csv_auto($urls)",
        {"urls": urls},
    )
    con.execute(
        f"""
        alter table {SCHEMA_NAME}.matches
        alter tourney_date set data type date using
            cast(strptime(cast(tourney_date as varchar), '%Y%m%d') as date)
        """
    )


def load_countries(con: duckdb.DuckDBPyConnection) -> None:
    log.info(colored(f"Downloading countries data…", "magenta"))
    response = requests.get(COUNTRIES_API_URL)
    response.raise_for_status()
    with open(COUNTRIES_CACHE, "w") as f:
        json.dump(response.json(), f, indent=2)

    create_schema(con, SCHEMA_NAME)
    con.execute(
        f"create or replace table {SCHEMA_NAME}.countries as select * from read_json_auto($path)",
        {"path": COUNTRIES_CACHE},
    )


def main() -> None:
    players_urls = get_repo_file_urls(r"atp_players\.csv")
    matches_urls = get_repo_file_urls(r"atp_matches_\d{4}\.csv")

    if not players_urls:
        raise RuntimeError("ATP players file not found in repo.")
    if not matches_urls:
        raise RuntimeError("No ATP matches files found in repo.")

    con = get_duckdb_connection()
    try:
        load_players(con, players_urls)
        load_matches(con, matches_urls)
        load_countries(con)
    finally:
        con.close()

    log.info(colored("Data import complete!", "cyan"))


if __name__ == "__main__":
    main()
