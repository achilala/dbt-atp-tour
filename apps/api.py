#!/usr/bin/env python3
"""
FastAPI REST API for ATP Tour data.

Usage:
    uv run uvicorn apps.api:app --reload
"""
__author__ = "achilala"
__version__ = "0.1.0"

import logging

import duckdb
from fastapi import FastAPI

log = logging.getLogger(__name__)

app = FastAPI(title="ATP Tour API", version="0.1.0")
_db = duckdb.connect("analytics.duckdb", read_only=True)

_GET_PLAYERS_SQL = """
    select player_id
          ,player_name
          ,first_name
          ,last_name
      from mart.dim_player
     where lower(player_name) like lower('%' || $player_name || '%')
     order by player_name
"""


def _query(sql: str, params: dict) -> list[dict]:
    return _db.execute(sql, params).df().to_dict(orient="records")


@app.get("/")
async def root():
    return {"message": "Welcome to the ATP Tour API"}


@app.get("/players")
async def list_players():
    return _query(_GET_PLAYERS_SQL, {"player_name": ""})


@app.get("/players/{player_name}")
async def search_players(player_name: str):
    return _query(_GET_PLAYERS_SQL, {"player_name": player_name})
