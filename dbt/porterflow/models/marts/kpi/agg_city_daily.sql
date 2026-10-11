-- The ops team's daily scorecard: one row per city per IST booking date.
with trips as (
    select
        booked_date_ist,
        city_code,
        count(*)                                                                as trips_booked,
        count(*) filter (where trip_status = 'completed')                       as trips_completed,
        count(*) filter (where trip_status = 'cancelled')                       as trips_cancelled,
        count(*) filter (where trip_status = 'in_progress')                     as trips_in_progress,
        count(*) filter (where cancelled_by = 'customer')                       as cancelled_by_customer,
        count(*) filter (where cancelled_by = 'driver')                         as cancelled_by_driver,
        count(*) filter (where cancelled_by = 'system')                         as cancelled_by_system,
        count(*) filter (where cancellation_stage = 'before_assignment')        as cancelled_before_assignment,
        sum(fare_amount) filter (where trip_status = 'completed')               as gmv_inr,
        avg(fare_amount) filter (where trip_status = 'completed')               as avg_fare_inr,
        median(time_to_assign_sec)                                              as median_time_to_assign_sec,
        avg(dead_km) filter (where trip_status = 'completed')                   as avg_dead_km,
        sum(dead_km) filter (where trip_status = 'completed')                   as dead_km,
        sum(distance_km) filter (where trip_status = 'completed')               as loaded_km,
        avg(distance_km) filter (where trip_status = 'completed')               as avg_trip_km,
        count(distinct driver_id)                                               as drivers_with_trips,
        count(*) filter (where is_speed_anomaly)                                as speed_anomalies,
        sum(late_event_count)                                                   as late_events,
        sum(event_count)                                                        as events
    from {{ ref('fct_trips') }}
    where booked_date_ist >= {{ history_start() }}
    group by 1, 2
),

drivers as (
    select
        activity_date_ist as booked_date_ist,
        city_code,
        count(*) filter (where online_minutes > 0)                              as drivers_online,
        sum(busy_minutes) / nullif(sum(online_minutes), 0)                      as fleet_utilisation
    from {{ ref('agg_driver_daily') }}
    where activity_date_ist >= {{ history_start() }}
    group by 1, 2
)

select
    t.booked_date_ist,
    t.city_code,
    c.city_name,
    t.trips_booked,
    t.trips_completed,
    t.trips_cancelled,
    t.trips_in_progress,
    -- rates use finished trips only, so a day that is still settling is not penalised
    t.trips_completed / nullif(t.trips_completed + t.trips_cancelled, 0)        as completion_rate,
    t.trips_cancelled / nullif(t.trips_completed + t.trips_cancelled, 0)        as cancellation_rate,
    t.cancelled_by_customer,
    t.cancelled_by_driver,
    t.cancelled_by_system,
    t.cancelled_before_assignment,
    round(t.gmv_inr, 2)                                                         as gmv_inr,
    round(t.avg_fare_inr, 2)                                                    as avg_fare_inr,
    t.median_time_to_assign_sec,
    round(t.avg_dead_km, 3)                                                     as avg_dead_km,
    round(t.dead_km / nullif(t.dead_km + t.loaded_km, 0), 4)                    as dead_km_share,
    round(t.avg_trip_km, 3)                                                     as avg_trip_km,
    t.drivers_with_trips,
    d.drivers_online,
    round(d.fleet_utilisation, 4)                                               as fleet_utilisation,
    t.speed_anomalies,
    round(t.late_events / nullif(t.events, 0), 4)                               as late_event_share
from trips as t
left join drivers as d using (booked_date_ist, city_code)
left join {{ ref('dim_city') }} as c using (city_code)
