-- A trip is completed iff it has a delivery, cancelled iff it has a cancellation and no delivery.
select trip_id, trip_status, delivered_at_utc, cancelled_at_utc
from {{ ref('fct_trips') }}
where (trip_status = 'completed' and delivered_at_utc is null)
   or (trip_status = 'cancelled' and (cancelled_at_utc is null or delivered_at_utc is not null))
   or (trip_status = 'in_progress' and (delivered_at_utc is not null or cancelled_at_utc is not null))
