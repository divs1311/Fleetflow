{% snapshot snap_drivers %}
{#-
  SCD Type 2 history of drivers. 'timestamp' strategy keyed on the source's
  updated_at, so dbt_valid_from is the moment the change happened in the source
  system (not the moment this job ran). That keeps history correct when the
  pipeline is backfilled days later, and makes re-runs no-ops.
-#}
{{
  config(
    target_schema = 'snapshots',
    unique_key = 'driver_id',
    strategy = 'timestamp',
    updated_at = 'updated_at_utc'
  )
}}
select
    driver_id,
    full_name,
    city_code,
    vehicle_id,
    vehicle_type,
    shift_type,
    status,
    rating,
    onboarded_at_utc,
    updated_at_utc
from {{ ref('int_drivers_current') }}
{% endsnapshot %}
