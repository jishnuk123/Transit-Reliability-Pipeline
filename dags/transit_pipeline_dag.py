"""
Transit Reliability Pipeline DAG.

Orchestrates the near-real-time ingestion of MBTA Trip Updates:
  1. Fetch and flatten the live feed, writing a local JSON Lines file.
  2. Load that file into Snowflake's RAW.TRIP_UPDATES table.

Uses Airflow's TaskFlow API (@dag / @task decorators), which lets tasks
pass data to each other as normal Python function return values --
Airflow handles the underlying XCom storage automatically.
"""

from datetime import datetime

from airflow.decorators import dag, task

# These imports work because Astro mounts the project's `include/`
# folder into the container, and it's on the Python path automatically.
from include.scripts.fetch_trip_updates import fetch_and_write
from include.scripts.load_to_snowflake import load_file


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

    # Task dependency: load_task receives fetch_task's return value
    # directly as its argument. Airflow infers from this that fetch_task
    # must run first -- no separate ">>" needed, since the data
    # dependency IS the task dependency here.
    filepath = fetch_task()
    load_task(filepath)


# Instantiate the DAG so Airflow can discover it.
transit_trip_updates_pipeline()