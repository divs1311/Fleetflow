-- Lifecycle order booked <= assigned <= picked up <= delivered must hold, except for
-- trips explicitly flagged has_out_of_order_events (device clock skew), which are kept but marked.
select trip_id
from {{ ref('fct_trips') }}
where not has_out_of_order_events
  and (assigned_at_utc < booked_at_utc
       or picked_up_at_utc < assigned_at_utc
       or delivered_at_utc < picked_up_at_utc)
