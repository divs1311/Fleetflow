-- Output of the PySpark job, exposed in the warehouse with proper types.
-- Grain: driver x IST hour.
select
    driver_id,
    city_code,
    vehicle_type,
    cast(event_date as date)                                        as activity_date_ist,
    hour_of_day,
    cast(event_date as date) + to_hours(hour_of_day)                as hour_start_ist,
    online_seconds,
    busy_seconds,
    trips_started,
    utilisation,
    is_busy_outside_shift
from {{ source('curated', 'driver_hourly_utilisation') }}
