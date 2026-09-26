-- Mart model: on-time performance summary by route.
--
-- On-time definition (industry standard, matches MBTA's own established
-- schedule-adherence thresholds): arriving no more than 2 minutes early
-- and no more than 5 minutes late. Early arrivals are NOT automatically
-- "good" -- a bus that leaves early can strand a rider who arrives on
-- schedule, so the window is intentionally asymmetric.

with trip_performance as (
    select * from {{ ref('int_trip_performance') }}
    where arrival_delay_seconds is not null
),

flagged as (
    select
        *,
        case
            when arrival_delay_seconds >= -120 and arrival_delay_seconds <= 300
                then 1
            else 0
        end as is_on_time
    from trip_performance
)

select
    route_id,
    count(*) as total_stop_events,
    round(avg(arrival_delay_seconds), 1) as avg_delay_seconds,
    round(median(arrival_delay_seconds), 1) as median_delay_seconds,
    sum(is_on_time) as on_time_count,
    round(100.0 * sum(is_on_time) / count(*), 1) as pct_on_time

from flagged
group by route_id
order by pct_on_time asc