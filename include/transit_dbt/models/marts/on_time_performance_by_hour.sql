-- Mart model: on-time performance summary by hour of day.
-- Uses the scheduled arrival hour (not the actual arrival hour) as the
-- grouping key, since we want to know "how reliable is service that's
-- SUPPOSED to run at hour X" -- e.g. to see if rush hour is worse.
--
-- Same on-time definition as on_time_performance_by_route: no more than
-- 2 minutes early, no more than 5 minutes late.

with trip_performance as (
    select * from {{ ref('int_trip_performance') }}
    where arrival_delay_seconds is not null
),

flagged as (
    select
        *,
        hour(scheduled_arrival_timestamp) as scheduled_hour,
        case
            when arrival_delay_seconds >= -120 and arrival_delay_seconds <= 300
                then 1
            else 0
        end as is_on_time
    from trip_performance
)

select
    scheduled_hour,
    count(*) as total_stop_events,
    round(avg(arrival_delay_seconds), 1) as avg_delay_seconds,
    round(median(arrival_delay_seconds), 1) as median_delay_seconds,
    sum(is_on_time) as on_time_count,
    round(100.0 * sum(is_on_time) / count(*), 1) as pct_on_time

from flagged
group by scheduled_hour
order by scheduled_hour