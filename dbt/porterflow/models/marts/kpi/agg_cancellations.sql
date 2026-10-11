-- Cancellations by reason, stage and owning team: where to act, and who acts.
select
    f.booked_date_ist,
    f.city_code,
    f.vehicle_type,
    f.cancelled_by,
    f.cancellation_reason,
    coalesce(r.stage, f.cancellation_stage)   as stage,
    coalesce(r.owner_team, 'unmapped')        as owner_team,
    coalesce(r.is_controllable, false)        as is_controllable,
    count(*)                                  as cancellations,
    avg(f.time_to_assign_sec)                 as avg_time_to_assign_sec,
    avg(f.dead_km)                            as avg_dead_km_at_cancel
from {{ ref('fct_trips') }} as f
left join {{ ref('cancellation_reasons') }} as r using (cancellation_reason)
where f.trip_status = 'cancelled'
  and f.booked_date_ist >= {{ history_start() }}
group by all
