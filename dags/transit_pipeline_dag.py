"""
Transit Reliability Pipeline DAG.

Orchestrates the near-real-time ingestion of MBTA Trip Updates:
  1. Fetch and flatten the live feed, writing a local JSON Lines file.
  2. Load that file into Snowflake's RAW.TRIP_UPDATES table.

Uses Airflow's TaskFlow API (@dag / @task decorators), which lets tasks
pass data to each other as normal Python function return values --
Airflow handles the underlying XCom storage automatically.
"""

import subprocess
from datetime import datetime

from airflow.decorators import dag, task

# These imports work because Astro mounts the project's `include/`
# folder into the container, and it's on the Python path automatically.
from include.scripts.fetch_trip_updates import fetch_and_write
from include.scripts.load_to_snowflake import load_file

DBT_PROJECT_DIR = "/usr/local/airflow/include/transit_dbt"


@dag(
    dag_id="transit_trip_updates_pipeline",
    schedule="*/15 * * * *",  # every 15 minutes, in standard cron syntax
    start_date=datetime(2026, 9, 1),
    catchup=False,  # don't backfill runs for the period before the DAG was turned on
    tags=["transit", "elt"],
)
def transit_trip_updates_pipeline():

    @task()
    def fetch_task():
        """Fetch and flatten the live MBTA Trip Updates feed."""
        return fetch_and_write()

    @task()
    def load_task(filepath: str):
        """Load the fetched file into Snowflake."""
        load_file(filepath)

    @task()
    def dbt_run_task():
        """
        Run dbt to transform the newly loaded raw data.
        Uses subprocess since dbt is a command-line tool, not a Python
        function we can import and call directly.
        """
        result = subprocess.run(
            ["dbt", "run", "--project-dir", DBT_PROJECT_DIR],
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError("dbt run failed")

    # Task dependencies: load_task receives fetch_task's return value
    # directly as its argument, so Airflow infers fetch_task must run
    # first. dbt_run_task doesn't need any data FROM load_task, but it
    # does need to run AFTER it -- so we declare that ordering explicitly
    # with >>, since there's no data flow to infer it from automatically.
    filepath = fetch_task()
    load_task(filepath) >> dbt_run_task()


# Instantiate the DAG so Airflow can discover it.
transit_trip_updates_pipeline()