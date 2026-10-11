-- Grain: driver x IST day. Busy time = assignment -> delivery/cancellation.
with trips as (
    select
        driver_id,
        cast({{ to_ist('assigned_at_utc') }} as date)                                 as activity_date_ist,
        count(*)                                                                      as trips_assigned,
        count(*) filter (where trip_status = 'completed')                             as trips_completed,
        count(*) filter (where cancelled_by = 'driver')                               as trips_cancelled_by_driver,
        sum(fare_amount) filter (where trip_status = 'completed')                     as gmv_inr,
        sum(dead_km)                                                                  as dead_km,
        sum(distance_km) filter (where trip_status = 'completed')                     as loaded_km,
        sum(date_diff('second', assigned_at_utc,
                      coalesce(delivered_at_utc, cancelled_at_utc))) / 60.0           as busy_minutes
    from {{ ref('fct_trips') }}
    where driver_id is not null
      and cast({{ to_ist('assigned_at_utc') }} as date) >= {{ history_start() }}
    group by 1, 2
),

shifts as (
    select driver_id, shift_date_ist as activity_date_ist, any_value(city_code) as city_code,
           sum(online_minutes) as online_minutes, bool_or(is_logout_missing) as has_missing_logout
    from {{ ref('fct_driver_shifts') }}
    where shift_date_ist >= {{ history_start() }}
    group by 1, 2
)

select
    coalesce(s.driver_id, t.driver_id)                                    as driver_id,
    coalesce(s.activity_date_ist, t.activity_date_ist)                    as activity_date_ist,
    coalesce(s.city_code, d.city_code)                                    as city_code,
    d.vehicle_type,
    coalesce(t.trips_assigned, 0)                                         as trips_assigned,
    coalesce(t.trips_completed, 0)                                        as trips_completed,
    coalesce(t.trips_cancelled_by_driver, 0)                              as trips_cancelled_by_driver,
    coalesce(t.gmv_inr, 0)                                                as gmv_inr,
    round(coalesce(t.gmv_inr, 0) * 0.8, 2)                                as est_driver_earnings_inr,
    round(coalesce(t.dead_km, 0), 2)                                      as dead_km,
    round(coalesce(t.loaded_km, 0), 2)                                    as loaded_km,
    round(coalesce(t.busy_minutes, 0), 1)                                 as busy_minutes,
    round(coalesce(s.online_minutes, 0), 1)                               as online_minutes,
    case when s.online_minutes > 0
         then least(1.0, coalesce(t.busy_minutes, 0) / s.online_minutes) end as utilisation,
    coalesce(s.has_missing_logout, false)                                 as has_missing_logout
from shifts as s
full outer join trips as t
    on s.driver_id = t.driver_id and s.activity_date_ist = t.activity_date_ist
left join {{ ref('dim_driver') }} as d
    on d.driver_id = coalesce(s.driver_id, t.driver_id) and d.is_current
