# dbt ATP Tour v2

A dbt project that transforms raw ATP tennis tour data into a star-schema analytics layer backed by DuckDB, with a FastAPI REST API and a Streamlit head-to-head comparison app.

## What it does

Raw source data (players, matches, countries) is staged, enriched, and modelled into a clean set of dimensions and facts:

| Layer | Schema | Models |
|---|---|---|
| Staging | `staging` | `stg_atp_tour__players`, `stg_atp_tour__matches`, `stg_atp_tour__countries` |
| Intermediate | `intermediate` | `int_atp__matches_enriched`, `int_atp__player_career_stats` |
| Core marts | `mart` | `dim_date`, `dim_time`, `dim_player`, `dim_tournament`, `fct_match` |
| Reporting | `reporting` | `rpt_match_summary` |

The output lives in a local `analytics.duckdb` file. Two apps sit on top of it:

- **FastAPI** (`apps/api.py`) — REST endpoint for player search
- **Streamlit** (`apps/streamlit_app.py`) — head-to-head player comparison with match history, surface and round breakdowns, and trend charts

## Requirements

- Python >= 3.12
- [uv](https://docs.astral.sh/uv/) (package manager)
- A `profiles.yml` configured for the `dbt_atp_tour` profile (not committed — see dbt docs)

## Setup

```bash
# Install dependencies
uv sync

# Install dbt packages
uv run dbt deps

# Load seed data
uv run dbt seed

# Build all models
uv run dbt build
```

This produces `analytics.duckdb` in the project root.

## Running the apps

**FastAPI**

```bash
uv run uvicorn apps.api:app --reload
```

Endpoints:
- `GET /players` — list all players
- `GET /players/{player_name}` — search players by name

**Streamlit**

```bash
uv run streamlit run apps/streamlit_app.py
```

Opens a browser UI where you can search two players and see their head-to-head record, career stats, match history, and charts.

## Useful dbt commands

```bash
# Run all models
uv run dbt run

# Run tests
uv run dbt test

# Run a specific model and its dependencies
uv run dbt run --select +fct_match

# Compile without executing
uv run dbt compile
```

## Project variables

| Variable | Default | Description |
|---|---|---|
| `dbt_timezone` | `Pacific/Auckland` | Timezone for date/time calculations |
| `calendar_start_date` | `1800-01-01` | Start of the date dimension |
| `cm_to_inches` | `0.0328084` | Unit conversion factor |
