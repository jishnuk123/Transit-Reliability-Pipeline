"""
Static Schedule Refresh DAG.

Refreshes MBTA's static GTFS schedule (stops, trips, stop_times) once
per day. This runs on a much less frequent schedule than the real-time
Trip Updates pipeline, since published schedules only change every few
weeks, not every few minutes -- running this more often would waste
compute for no benefit.
"""

from datetime import datetime

from airflow.decorators import dag, task

from include.scripts.fetch_static_schedule import run_static_schedule_load
from include.scripts.alerts import send_failure_alert


@dag(
    dag_id="static_schedule_refresh",
    schedule="0 3 * * *",  # once a day, at 3:00 AM
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["transit", "elt", "static"],
    default_args={
        "on_failure_callback": send_failure_alert,
    },
)
def static_schedule_refresh():

    @task()
    def load_static_schedule_task():
        run_static_schedule_load()

    load_static_schedule_task()


static_schedule_refresh()