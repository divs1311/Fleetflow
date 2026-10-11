-- Calendar dimension covering every IST date that has trip data.
with bounds as (
    select
        min(cast({{ to_ist('event_ts_utc') }} as date)) as first_day,
        max(cast({{ to_ist('event_ts_utc') }} as date)) as last_day
    from {{ ref('int_trip_events') }}
)

select
    cast(d.day as date)                          as date_day,
    extract('isodow' from d.day)::int            as iso_day_of_week,
    dayname(d.day)                               as day_name,
    extract('isodow' from d.day) in (6, 7)       as is_weekend,
    cast(date_trunc('week', d.day) as date)      as week_start,
    cast(date_trunc('month', d.day) as date)     as month_start
from bounds,
     range(bounds.first_day, bounds.last_day + interval 1 day, interval 1 day) as d(day)
