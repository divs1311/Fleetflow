{{ config(severity = 'warn') }}
-- The app sometimes reports a driver's previous vehicle after a switch (stale cache).
-- Expected at a low rate, so this only warns; the warehouse quality gate fails the run
-- if the mismatch *rate* for the day exceeds 5%.
select trip_id, driver_id, vehicle_id
from {{ ref('fct_trips') }}
where is_vehicle_mismatch
