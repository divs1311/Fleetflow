-- Supply vs demand by city and IST hour: the heatmap behind the evening supply gap.
with supply as (
    select
        activity_date_ist,
        hour_of_day,
        city_code,
        count(*) filter (where online_seconds > 0)                   as drivers_online,
        sum(busy_seconds) / nullif(sum(online_seconds), 0)           as utilisation
    from {{ ref('fct_driver_hourly_utilisation') }}
    group by 1, 2, 3
),

demand as (
    select
        booked_date_ist     as activity_date_ist,
        booked_hour_ist     as hour_of_day,
        city_code,
        count(*)                                                     as trips_booked,
        count(*) filter (where trip_status = 'completed')            as trips_completed,
        count(*) filter (where cancellation_reason in ('no_driver_found', 'long_wait_for_assignment'))
                                                                     as supply_cancellations
    from {{ ref('fct_trips') }}
    where booked_date_ist >= {{ history_start() }}
    group by 1, 2, 3
)

select
    coalesce(s.activity_date_ist, d.activity_date_ist)   as activity_date_ist,
    coalesce(s.hour_of_day, d.hour_of_day)               as hour_of_day,
    coalesce(s.city_code, d.city_code)                   as city_code,
    coalesce(d.trips_booked, 0)                          as trips_booked,
    coalesce(d.trips_completed, 0)                       as trips_completed,
    coalesce(d.supply_cancellations, 0)                  as supply_cancellations,
    coalesce(s.drivers_online, 0)                        as drivers_online,
    round(s.utilisation, 4)                              as utilisation,
    round(coalesce(d.trips_booked, 0) / nullif(s.drivers_online, 0), 3) as bookings_per_online_driver
from supply as s
full outer join demand as d
    on s.activity_date_ist = d.activity_date_ist and s.hour_of_day = d.hour_of_day and s.city_code = d.city_code
