"""
Stage 1: Ingestion script

Fetches the MBTA Trip Updates GTFS-realtime feed, decodes it from
Protocol Buffers, flattens the nested structure into one row per
stop-time-update, and writes the result as JSON Lines to a local
"raw" data folder.

This is a parsing/format step (protobuf -> tabular rows), not a
business transformation -- no calculations or joins happen here.
That logic belongs later, in dbt.
"""

import json
import os
from datetime import datetime, timezone

import requests
from google.transit import gtfs_realtime_pb2

TRIP_UPDATES_URL = "https://cdn.mbta.com/realtime/TripUpdates.pb"
RAW_DATA_DIR = "data/raw/trip_updates"


def fetch_trip_updates():
    """Fetch the raw Protocol Buffer bytes from MBTA's feed."""
    response = requests.get(TRIP_UPDATES_URL)
    response.raise_for_status()
    return response.content


def decode_trip_updates(raw_bytes):
    """Decode raw Protocol Buffer bytes into a GTFS-realtime FeedMessage object."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw_bytes)
    return feed


def flatten_feed(feed):
    """
    Convert the nested FeedMessage structure into a flat list of
    dictionaries -- one dictionary per stop-time-update.

    This is where we go from "one object per trip, containing many
    stops" to "one row per stop," which is the shape a SQL table
    (and therefore Snowflake) actually needs.
    """
    records = []

    for entity in feed.entity:
        trip_update = entity.trip_update
        trip = trip_update.trip

        # Some trip updates have no vehicle assigned yet -- handle that safely
        vehicle_id = trip_update.vehicle.id if trip_update.HasField("vehicle") else None

        for stu in trip_update.stop_time_update:
            record = {
                "trip_id": trip.trip_id,
                "route_id": trip.route_id,
                "direction_id": trip.direction_id,
                "start_date": trip.start_date,
                "start_time": trip.start_time,
                "vehicle_id": vehicle_id,
                "stop_id": stu.stop_id,
                "stop_sequence": stu.stop_sequence,
                # arrival/departure are optional -- only present if the field was set
                "arrival_time": stu.arrival.time if stu.HasField("arrival") else None,
                "departure_time": stu.departure.time if stu.HasField("departure") else None,
                "schedule_relationship": gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(
                    stu.schedule_relationship
                ),
                "feed_timestamp": trip_update.timestamp,
            }
            records.append(record)

    return records


def write_records(records):
    """Write the flattened records to a local JSON Lines file, named by fetch time."""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)

    fetch_time = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filepath = os.path.join(RAW_DATA_DIR, f"trip_updates_{fetch_time}.jsonl")

    with open(filepath, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    return filepath


def main():
    print("Fetching MBTA Trip Updates feed...")
    raw_bytes = fetch_trip_updates()
    print(f"Fetched {len(raw_bytes)} bytes.")

    print("Decoding Protocol Buffer data...")
    feed = decode_trip_updates(raw_bytes)
    print(f"Feed contains {len(feed.entity)} trip entities.")

    print("Flattening into row-shaped records...")
    records = flatten_feed(feed)
    print(f"Produced {len(records)} stop-time-update records.")

    print("Writing to local raw data folder...")
    filepath = write_records(records)
    print(f"Wrote records to: {filepath}")


if __name__ == "__main__":
    main()