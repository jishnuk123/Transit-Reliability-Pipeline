-- Staging model for the static GTFS stops table.
-- Purpose: light cleanup -- select only the columns we actually need,
-- and cast lat/lon from strings (raw layer is all VARCHAR) into numbers.
 
select
    stop_id,
    stop_name,
    stop_lat::float as stop_lat,
    stop_lon::float as stop_lon,
    municipality,
    wheelchair_boarding
 
from {{ source('raw', 'stops') }}