-- Raw layer schema definitions for the Transit Reliability Pipeline.
-- This is the "landing zone" layer in our ELT design: data lands here
-- close to its original shape, with minimal transformation. Business
-- logic (delay calculations, joins, aggregations) happens later in dbt.

USE WAREHOUSE TRANSIT_WH;
USE DATABASE TRANSIT_PIPELINE;
USE SCHEMA RAW;

-- Stores flattened MBTA GTFS-realtime Trip Updates.
-- One row per stop-time-update (i.e., one row per stop along a trip),
-- not one row per trip -- see fetch_trip_updates.py for the flattening logic.
CREATE TABLE IF NOT EXISTS RAW.TRIP_UPDATES (
    trip_id STRING,
    route_id STRING,
    direction_id INTEGER,
    start_date STRING,
    start_time STRING,
    vehicle_id STRING,
    stop_id STRING,
    stop_sequence INTEGER,
    arrival_time INTEGER,          -- Unix timestamp; converted to a real timestamp in dbt
    departure_time INTEGER,        -- Unix timestamp; converted to a real timestamp in dbt
    schedule_relationship STRING,  -- e.g. SCHEDULED, SKIPPED
    feed_timestamp INTEGER,        -- Unix timestamp of when this prediction was generated
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()  -- when this row was loaded into Snowflake
);