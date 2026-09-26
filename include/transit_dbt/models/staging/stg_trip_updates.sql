
-- Staging model for real-time Trip Updates.
-- Purpose: light cleanup only -- casting Unix timestamps (stored as raw
-- integers in the raw layer) into real TIMESTAMP types. No joins, no
-- aggregation -- that logic belongs in later layers.
 
select
    trip_id,
    route_id,
    direction_id,
    start_date,
    start_time,
    vehicle_id,
    stop_id,
    stop_sequence,
 
    -- Unix timestamps (seconds since 1970-01-01, always UTC) converted to
    -- real timestamps, then shifted into US Eastern time -- MBTA's static
    -- GTFS schedule times are published in local time with no timezone
    -- attached, so both sides of the eventual comparison need to speak
    -- the same timezone or delay calculations will be off by several hours.
    convert_timezone('UTC', 'America/New_York', to_timestamp_ntz(arrival_time, 0))   as arrival_timestamp,
    convert_timezone('UTC', 'America/New_York', to_timestamp_ntz(departure_time, 0)) as departure_timestamp,
    convert_timezone('UTC', 'America/New_York', to_timestamp_ntz(feed_timestamp, 0)) as feed_timestamp,
 
    schedule_relationship,
    ingested_at
 
from {{ source('raw', 'trip_updates') }}