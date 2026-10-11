{{ config(severity = 'warn') }}
-- Drivers should not be busy for longer than they were logged in. A handful of
-- violations is expected (late login events, missing logouts) and is surfaced as a warning.
select driver_id, activity_date_ist, busy_minutes, online_minutes
from {{ ref('agg_driver_daily') }}
where online_minutes > 0 and busy_minutes > online_minutes + 30
