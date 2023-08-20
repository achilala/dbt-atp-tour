# What this project is about

This is a `dbt`, `DuckDB` and `Python` project which uses the [ATP Tennis Rankings, Results, and Stats](https://github.com/JeffSackmann/tennis_atp) dataset that is publicly available on Github, to demonstrate how to structure a dimensional model suitable for analytics. Another helpful resource is this [Youtube video](https://www.youtube.com/watch?v=cp7qRN9jd8I) with regard to how to get the data into a DuckDB database.

## Pre-requisites

For this project make sure you have following installed:

-   [Git](https://git-scm.com/downloads)
-   [Python](https://www.python.org/downloads/)
-   [DuckDB CLI](https://duckdb.org/docs/installation/index)

## Getting started with this project

Clone the [ATP Tour project](https://github.com/achilala/dbt-atp-tour) to somewhere on your local directory
```bash
git clone https://github.com/achilala/dbt-atp-tour.git
cd dbt-atp-tour
```

Create a virtual environment for your python dependencies and activate it. Your python dependencies will be installed here.
```bash
python3 -m venv .venv/dbt-atp-tour
source .venv/dbt-atp-tour/bin/activate
```

Install the python dependencies
```bash
pip install -r requirements.txt
```

Some of the installed modules require reactivating your virtual environment to take effect
```bash
deactivate
source .venv/dbt-atp-tour/bin/activate
```

Running this python script will read the ATP tour data into a duckdb database called `atp_tour.duckdb`
```python 
python3 download_atp_tour_data.py
```

A new file called `atp_tour.duckdb` should appear in the folder, and this is the `DuckDB` database file. The reason why `DuckDB` is my-go-to database is because it's very light, simple to setup and built for analytical processing.

Before running `dbt` makes sure the `profiles.yml` file is setup corrently. The `path` in the file should point to your duckdb database and on mine it looks something like this `atp_tour.duckdb`.

Test your connection and adjust your `profiles.yml` settings accordingly until you get a successful test.
```bash
dbt debug
```

Run the dbt project to build your models for analysis
```bash
dbt clean && dbt deps && dbt build
```

To generate and view the project documentation run the following.
```bash
dbt docs generate && dbt docs serve
```

A browser should open with the docs site or [click here](http://127.0.0.1:8080/#!/overview). To cancel press the following on the keyboard
```bash
^c
```

Use the `DuckDB CLI` to query the `DuckDB` database. Download and unzip the CLI
```bash
curl -OL https://github.com/duckdb/duckdb/releases/download/v0.7.1/duckdb_cli-osx-universal.zip
unzip duckdb_cli-osx-universal.zip
```

Open the database
```bash
./duckdb --readonly atp_tour.duckdb
```

Sample query
```sql
summarize mart.dim_player;

select *
  from mart.dim_player
 order by dim_player_key;
```

Now that the data is modeled, we can now run the streamlit app
```sh
streamlit run atp_tour_app.py
```

For BI and analysing the data we'll use Metabase, run Docker to setup it up
```sh
docker-compose up --build
```

For consuming the data from other apps, run the following command to start the API service
```sh
uvicorn atp_tour_api:app --reload
```