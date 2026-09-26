-- Intermediate model: the core of the pipeline's analysis.
-- Joins real-time predicted arrival/departure times against the
-- scheduled times, and calculates delay in seconds for each stop.
--
-- Delay = actual_time - scheduled_time
--   positive -> running late
--   negative -> running early
--   ~0       -> on time

with trip_updates as (
    select * from {{ ref('stg_trip_updates') }}
),

stop_times as (
    select * from {{ ref('stg_stop_times') }}
),

-- Step 1: join real-time updates to their matching scheduled stop,
-- matched on both trip_id AND stop_id (a trip has many stops, so we
-- need both fields to correctly pair the right stop).
joined as (
    select
        tu.trip_id,
        tu.route_id,
        tu.direction_id,
        tu.start_date,
        tu.stop_id,
        tu.stop_sequence,
        tu.vehicle_id,
        tu.schedule_relationship,
        tu.arrival_timestamp   as actual_arrival_timestamp,
        tu.departure_timestamp as actual_departure_timestamp,
        st.arrival_hour,
        st.arrival_minute,
        st.arrival_second,
        st.departure_hour,
        st.departure_minute,
        st.departure_second
    from trip_updates tu
    inner join stop_times st
        on tu.trip_id = st.trip_id
        and tu.stop_id = st.stop_id
),

-- Step 2: anchor the scheduled hour/minute/second offsets to the
-- trip's actual service date (start_date), using DATEADD so that
-- values past 24:00:00 (e.g. hour 25) roll over into the next day
-- automatically, without needing manual rollover logic.
with_scheduled_timestamps as (
    select
        *,
        dateadd(
            second,
            (arrival_hour * 3600) + (arrival_minute * 60) + arrival_second,
            to_timestamp_ntz(to_date(start_date, 'YYYYMMDD'))
        ) as scheduled_arrival_timestamp,

        dateadd(
            second,
            (departure_hour * 3600) + (departure_minute * 60) + departure_second,
            to_timestamp_ntz(to_date(start_date, 'YYYYMMDD'))
        ) as scheduled_departure_timestamp
    from joined
)

-- Step 3: calculate delay -- the actual payoff of this whole pipeline.
select
    trip_id,
    route_id,
    direction_id,
    stop_id,
    stop_sequence,
    vehicle_id,
    schedule_relationship,

    scheduled_arrival_timestamp,
    actual_arrival_timestamp,
    datediff(second, scheduled_arrival_timestamp, actual_arrival_timestamp)
        as arrival_delay_seconds,

    scheduled_departure_timestamp,
    actual_departure_timestamp,
    datediff(second, scheduled_departure_timestamp, actual_departure_timestamp)
        as departure_delay_seconds

from with_scheduled_timestamps