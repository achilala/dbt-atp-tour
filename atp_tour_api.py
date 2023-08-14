#!/usr/bin/env python3
"""
This script runs the FastAPI API for ATP Tour data.
"""
__author__ = "achilala"
__version__ = "0.0.1"

import logging
from fastapi import FastAPI
import duckdb

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

app = FastAPI()
atp_tour = duckdb.connect("atp_tour.duckdb", read_only=True)

get_players_query = """
    with players as (
        select player_id
              ,player_name
              ,first_name
              ,last_name
          from mart.dim_player
         where lower(player_name) like lower('%'||$player_name||'%')
    )
    select *
      from players
"""

def execute_query(query: str, params: dict):
    return atp_tour.execute(query, params).df().to_dict(orient="records")


@app.get("/")
async def root():
    return {"message": "Welcome to the ATP Tour API"}
    
@app.get("/players")
async def players_endpoint():
    return execute_query(get_players_query, {"player_name": ""})

@app.get("/players/{player_name}")
async def players_endpoint(player_name: str):
    return execute_query(get_players_query, {"player_name": player_name})