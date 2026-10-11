{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'append_new_columns'
  )
}}
-- Deduplicated driver login/logout events (same pattern as int_trip_events).

with new_rows as (
    select *
    from {{ ref('stg_driver_sessions') }}
    {% if is_incremental() %}
    where _loaded_at_utc > {{ incremental_watermark('_loaded_at_utc') }}
    {% endif %}
),

first_copy as (
    select *
    from new_rows
    qualify row_number() over (
        partition by event_id
        order by _loaded_at_utc, _kafka_partition, _kafka_offset
    ) = 1
)

select f.*
from first_copy as f
{% if is_incremental() %}
where not exists (select 1 from {{ this }} as t where t.event_id = f.event_id)
{% endif %}
