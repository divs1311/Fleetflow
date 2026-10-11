select
    event_id,
    schema_version,
    event_type,
    event_ts_utc,
    {{ to_ist('event_ts_utc') }}         as event_ts_ist,
    produced_at_utc,
    driver_id,
    vehicle_id,
    city_code,
    lat,
    lng,
    (lat is null or lng is null)         as is_gps_missing,
    cast(event_date as date)             as partition_event_date,
    _kafka_partition,
    _kafka_offset,
    _loaded_at_utc,
    _batch_id
from {{ source('raw', 'driver_sessions') }}
