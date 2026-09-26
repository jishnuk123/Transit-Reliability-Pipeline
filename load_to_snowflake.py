"""
Stage 3: Loading script

Reads a local JSON Lines file (produced by fetch_trip_updates.py) and
loads its rows into the Snowflake RAW.TRIP_UPDATES table using a
single batched insert.
"""

import json
import os
import sys

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

INSERT_SQL = """
    INSERT INTO RAW.TRIP_UPDATES (
        trip_id, route_id, direction_id, start_date, start_time,
        vehicle_id, stop_id, stop_sequence, arrival_time,
        departure_time, schedule_relationship, feed_timestamp
    )
    VALUES (%(trip_id)s, %(route_id)s, %(direction_id)s, %(start_date)s,
            %(start_time)s, %(vehicle_id)s, %(stop_id)s, %(stop_sequence)s,
            %(arrival_time)s, %(departure_time)s,
            %(schedule_relationship)s, %(feed_timestamp)s)
"""


def read_jsonl(filepath):
    """Read a JSON Lines file into a list of dictionaries."""
    records = []
    with open(filepath, "r") as f:
        for line in f:
            records.append(json.loads(line))
    return records


def load_records(records):
    """Connect to Snowflake and batch-insert all records into RAW.TRIP_UPDATES."""
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

    cursor = conn.cursor()
    cursor.executemany(INSERT_SQL, records)
    conn.commit()  # makes the inserted rows permanent

    cursor.close()
    conn.close()


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 load_to_snowflake.py <path_to_jsonl_file>")
        sys.exit(1)

    filepath = sys.argv[1]

    print(f"Reading records from {filepath}...")
    records = read_jsonl(filepath)
    print(f"Read {len(records)} records.")

    print("Loading records into Snowflake (RAW.TRIP_UPDATES)...")
    load_records(records)
    print("Load complete.")


if __name__ == "__main__":
    main()