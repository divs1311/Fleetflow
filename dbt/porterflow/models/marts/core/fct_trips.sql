{{
  config(
    materialized = 'incremental',
    unique_key = 'trip_id',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'append_new_columns'
  )
}}
{#-
  Grain: one row per trip (booking). This is an *accumulating snapshot* fact:
  a trip's row is rebuilt whenever a new event for it lands (e.g. the delivery
  event arrives 2 days late), moving it from in_progress to completed.

  Incremental logic: find trips touched by events loaded since the last run
  (minus a safety overlap), then recompute those trips from ALL their events and
  delete+insert them. Recomputing whole keys is what makes late data correct.
-#}

with changed_trips as (
    select distinct trip_id
    from {{ ref('int_trip_events') }}
    {% if is_incremental() %}
    where _loaded_at_utc > {{ incremental_watermark('last_event_loaded_at_utc') }}
    {% endif %}
),

events as (
    select e.*
    from {{ ref('int_trip_events') }} as e
    inner join changed_trips using (trip_id)
),

trip as (
    select
        trip_id,
        any_value(city_code)                                                            as city_code,
        arg_min(customer_id, event_ts_utc) filter (where event_type = 'trip_booked')    as customer_id,
        arg_min(vehicle_type, event_ts_utc)                                              as vehicle_type,
        min(event_ts_utc) filter (where event_type = 'trip_booked')                     as booked_event_at_utc,
        min(event_ts_utc)                                                                as first_event_at_utc,
        min(event_ts_utc) filter (where event_type = 'driver_assigned')                 as assigned_at_utc,
        min(event_ts_utc) filter (where event_type = 'trip_picked_up')                  as picked_up_at_utc,
        min(event_ts_utc) filter (where event_type = 'trip_delivered')                  as delivered_at_utc,
        min(event_ts_utc) filter (where event_type = 'trip_cancelled')                  as cancelled_at_utc,
        arg_min(driver_id, event_ts_utc) filter (where event_type = 'driver_assigned')  as driver_id,
        arg_min(vehicle_id, event_ts_utc) filter (where event_type = 'driver_assigned') as vehicle_id,
        arg_min(lat, event_ts_utc) filter (where event_type = 'driver_assigned')        as assigned_lat,
        arg_min(lng, event_ts_utc) filter (where event_type = 'driver_assigned')        as assigned_lng,
        arg_min(pickup_lat, event_ts_utc) filter (where event_type = 'trip_booked')     as pickup_lat,
        arg_min(pickup_lng, event_ts_utc) filter (where event_type = 'trip_booked')     as pickup_lng,
        arg_min(drop_lat, event_ts_utc) filter (where event_type = 'trip_booked')       as drop_lat,
        arg_min(drop_lng, event_ts_utc) filter (where event_type = 'trip_booked')       as drop_lng,
        arg_min(estimated_fare, event_ts_utc) filter (where event_type = 'trip_booked') as estimated_fare,
        arg_min(fare_amount, event_ts_utc) filter (where event_type = 'trip_delivered') as fare_amount,
        arg_min(distance_km, event_ts_utc) filter (where event_type = 'trip_delivered') as reported_distance_km,
        arg_min(payment_mode, event_ts_utc) filter (where event_type = 'trip_delivered') as payment_mode,
        arg_min(cancelled_by, event_ts_utc) filter (where event_type = 'trip_cancelled') as cancelled_by,
        arg_min(cancellation_reason, event_ts_utc) filter (where event_type = 'trip_cancelled') as cancellation_reason,
        count(*)                                                                         as event_count,
        count(*) filter (where is_gps_missing)                                           as gps_missing_event_count,
        count(*) filter (where is_late_arrival)                                          as late_event_count,
        max(_loaded_at_utc)                                                              as last_event_loaded_at_utc
    from events
    group by trip_id
),

derived as (
    select
        *,
        coalesce(booked_event_at_utc, first_event_at_utc)                                as booked_at_utc,
        booked_event_at_utc is null                                                      as is_booking_event_missing,
        case
            when delivered_at_utc is not null then 'completed'
            when cancelled_at_utc is not null then 'cancelled'
            else 'in_progress'
        end                                                                              as trip_status,
        case
            when cancelled_at_utc is null or delivered_at_utc is not null then null
            when assigned_at_utc is null then 'before_assignment'
            else 'after_assignment'
        end                                                                              as cancellation_stage,
        date_diff('second', picked_up_at_utc, delivered_at_utc)                          as trip_duration_sec,
        {{ haversine_km('assigned_lat', 'assigned_lng', 'pickup_lat', 'pickup_lng') }}
            * {{ var('road_factor') }}                                                   as dead_km
    from trip
),

with_flags as (
    select
        *,
        case when trip_duration_sec > 60
             then reported_distance_km / (trip_duration_sec / 3600.0) end                as implied_speed_kmph,
        coalesce(assigned_at_utc < booked_at_utc
                 or picked_up_at_utc < assigned_at_utc
                 or delivered_at_utc < picked_up_at_utc, false)                          as has_out_of_order_events
    from derived
)

select
    f.trip_id,
    cast({{ to_ist('f.booked_at_utc') }} as date)                                        as booked_date_ist,
    extract('hour' from {{ to_ist('f.booked_at_utc') }})::int                            as booked_hour_ist,
    f.city_code,
    f.vehicle_type,
    f.customer_id,
    f.driver_id,
    d.driver_sk,
    f.vehicle_id,
    f.trip_status,
    f.cancelled_by,
    f.cancellation_reason,
    f.cancellation_stage,
    f.booked_at_utc,
    f.assigned_at_utc,
    f.picked_up_at_utc,
    f.delivered_at_utc,
    f.cancelled_at_utc,
    date_diff('second', f.booked_at_utc, f.assigned_at_utc)                              as time_to_assign_sec,
    date_diff('second', f.assigned_at_utc, f.picked_up_at_utc)                           as assign_to_pickup_sec,
    f.trip_duration_sec,
    f.pickup_lat,
    f.pickup_lng,
    f.drop_lat,
    f.drop_lng,
    round(f.dead_km, 3)                                                                  as dead_km,
    f.reported_distance_km,
    coalesce(f.implied_speed_kmph > {{ var('max_plausible_speed_kmph') }}, false)        as is_speed_anomaly,
    case when coalesce(f.implied_speed_kmph > {{ var('max_plausible_speed_kmph') }}, false)
         then null else f.reported_distance_km end                                       as distance_km,
    round(f.implied_speed_kmph, 1)                                                       as implied_speed_kmph,
    f.estimated_fare,
    f.fare_amount,
    f.payment_mode,
    f.event_count,
    f.gps_missing_event_count,
    f.late_event_count,
    f.has_out_of_order_events,
    f.is_booking_event_missing,
    coalesce(f.vehicle_id <> d.vehicle_id, false)                                        as is_vehicle_mismatch,
    f.last_event_loaded_at_utc
from with_flags as f
left join {{ ref('dim_driver') }} as d
    on  f.driver_id = d.driver_id
    and f.assigned_at_utc >= d.valid_from_utc
    and f.assigned_at_utc <  d.valid_to_utc
