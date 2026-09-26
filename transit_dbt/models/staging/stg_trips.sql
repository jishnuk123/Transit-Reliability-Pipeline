-- Staging model for the static GTFS trips table.
-- Purpose: light cleanup -- select only the columns we actually need.
 
select
    trip_id,
    route_id,
    service_id,
    trip_headsign,
    direction_id::integer as direction_id
 
from {{ source('raw', 'trips') }}