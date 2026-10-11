-- SCD Type 2 driver dimension (one row per driver per version).
-- Facts join on driver_id AND event time between valid_from_utc and valid_to_utc,
-- so a trip always points to the vehicle the driver had *when the trip happened*.
with versions as (
    select
        *,
        row_number() over (partition by driver_id order by dbt_valid_from) as version_number
    from {{ ref('snap_drivers') }}
)

select
    dbt_scd_id                                           as driver_sk,
    driver_id,
    version_number,
    full_name,
    city_code,
    vehicle_id,
    vehicle_type,
    shift_type,
    status,
    rating,
    onboarded_at_utc,
    updated_at_utc                                       as source_updated_at_utc,
    -- the first known version is treated as valid since the beginning of time, so
    -- events older than the first extract still resolve to a driver row
    case when version_number = 1 then timestamp '1900-01-01' else dbt_valid_from end as valid_from_utc,
    coalesce(dbt_valid_to, timestamp '9999-12-31')       as valid_to_utc,
    dbt_valid_to is null                                 as is_current
from versions
