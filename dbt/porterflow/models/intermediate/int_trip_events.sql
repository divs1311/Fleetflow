{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'append_new_columns'
  )
}}
-- Deduplicated trip events: exactly one row per event_id (first copy that landed).
--
-- Incremental on *load time* (_loaded_at_utc), not on event time: an event that
-- happened 3 days ago but landed today is still picked up. Re-reading a small
-- overlap window and anti-joining on event_id makes re-runs idempotent.

with new_rows as (
    select *
    from {{ ref('stg_trip_events') }}
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
