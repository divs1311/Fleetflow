select
    city_code,
    city_name,
    state,
    region,
    city_tier,
    center_lat,
    center_lng,
    service_radius_km
from {{ ref('cities') }}
