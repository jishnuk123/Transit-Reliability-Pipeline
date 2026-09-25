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
 
-- ============================================================
-- Static GTFS schedule tables
-- ============================================================
-- These three tables come from MBTA's published static schedule zip
-- (https://cdn.mbta.com/MBTA_GTFS.zip), refreshed periodically (schedules
-- change every few weeks, not every few minutes -- unlike the real-time
-- feeds above). Each load OVERWRITES the previous snapshot, since these
-- tables represent "the current schedule as of now," not an accumulating
-- history like TRIP_UPDATES.
--
-- All columns are stored as VARCHAR in this raw layer, matching the
-- source CSV files exactly (no type casting yet -- that happens in dbt).
-- These tables were created automatically by write_pandas() in
-- fetch_static_schedule.py (auto_create_table=True), based on the
-- structure of each source file. Documented here for reference, since
-- the CREATE TABLE statements themselves were not hand-written.
 
-- RAW.STOPS: every physical stop/station in the system, with name and
-- lat/long coordinates. Columns include: stop_id, stop_code, stop_name,
-- stop_desc, platform_code, platform_name, stop_lat, stop_lon, zone_id,
-- stop_address, stop_url, level_id, location_type, parent_station,
-- wheelchair_boarding, municipality, on_street, at_street, vehicle_type.
 
-- RAW.TRIPS: every scheduled trip, linking a trip_id to a route and
-- service pattern. Columns include: route_id, service_id, trip_id,
-- trip_headsign, trip_short_name, direction_id, block_id, shape_id,
-- wheelchair_accessible, trip_route_type, route_pattern_id, bikes_allowed.
 
-- RAW.STOP_TIMES: the scheduled arrival/departure time for every trip at
-- every stop -- the direct counterpart to real-time TRIP_UPDATES, and
-- what we'll join against to calculate delay. Columns include: trip_id,
-- arrival_time, departure_time, stop_id, stop_sequence, stop_headsign,
-- pickup_type, drop_off_type, timepoint, checkpoint_id,
-- continuous_pickup, continuous_drop_off.