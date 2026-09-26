-- Staging model for the static GTFS stop_times table.
-- Purpose: convert GTFS's text-based time-of-day strings (which can
-- exceed 24:00:00 to represent trips continuing past midnight) into
-- real timestamps, without losing that rollover information.
--
-- We don't have an actual calendar date to anchor these to yet (that
-- comes from joining against trip_updates' start_date at the
-- intermediate layer), so for now we build each time as an interval
-- of hours/minutes/seconds -- the actual date anchoring happens next.
 
select
    trip_id,
    stop_id,
    stop_sequence::integer as stop_sequence,
 
    -- split "25:30:00" into its hour/minute/second parts
    split_part(arrival_time, ':', 1)::integer as arrival_hour,
    split_part(arrival_time, ':', 2)::integer as arrival_minute,
    split_part(arrival_time, ':', 3)::integer as arrival_second,
 
    split_part(departure_time, ':', 1)::integer as departure_hour,
    split_part(departure_time, ':', 2)::integer as departure_minute,
    split_part(departure_time, ':', 3)::integer as departure_second
 
from {{ source('raw', 'stop_times') }}