-- intermediate -> mart reconciliation: one fact row per distinct trip_id seen in events.
select e.trip_id
from (select distinct trip_id from {{ ref('int_trip_events') }}) as e
full outer join {{ ref('fct_trips') }} as f using (trip_id)
where e.trip_id is null or f.trip_id is null
