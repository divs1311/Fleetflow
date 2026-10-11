select
    vehicle_id,
    registration_no,
    vehicle_type,
    city_code,
    fuel_type,
    make_model,
    capacity_kg,
    created_at_utc,
    updated_at_utc,
    cast(extract_date as date) as extract_date,
    _extracted_at_utc
from {{ source('raw', 'master_vehicles') }}
