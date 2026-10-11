-- The published scorecard must add up to the fact table, per city and day.
with fact as (
    select booked_date_ist, city_code, count(*) as trips
    from {{ ref('fct_trips') }}
    where booked_date_ist >= {{ history_start() }}
    group by 1, 2
)
select f.booked_date_ist, f.city_code, f.trips, a.trips_booked
from fact as f
full outer join {{ ref('agg_city_daily') }} as a using (booked_date_ist, city_code)
where f.trips is distinct from a.trips_booked
