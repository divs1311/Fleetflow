-- Every version of every driver ever extracted (change log built from incremental extracts).
select
    driver_id,
    full_name,
    phone_masked,
    city_code,
    vehicle_id,
    vehicle_type,
    shift_type,
    status,
    rating,
    onboarded_at_utc,
    updated_at_utc,
    cast(extract_date as date) as extract_date,
    _extracted_at_utc
from {{ source('raw', 'master_drivers') }}
