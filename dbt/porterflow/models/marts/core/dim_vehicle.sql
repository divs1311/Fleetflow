-- SCD Type 1: one row per vehicle with its latest attributes, enriched with the tariff card.
select
    v.vehicle_id,
    v.registration_no,
    v.vehicle_type,
    t.vehicle_category,
    t.display_name       as vehicle_type_name,
    v.city_code,
    v.fuel_type,
    v.make_model,
    v.capacity_kg,
    t.base_fare_inr,
    t.per_km_inr,
    v.created_at_utc,
    v.updated_at_utc
from {{ ref('int_vehicles_current') }} as v
left join {{ ref('vehicle_types') }} as t using (vehicle_type)
