-- Glitched odometer readings must never reach distance KPIs.
select trip_id, distance_km, implied_speed_kmph
from {{ ref('fct_trips') }}
where distance_km is not null
  and implied_speed_kmph > {{ var('max_plausible_speed_kmph') }}
