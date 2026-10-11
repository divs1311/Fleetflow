-- Row-count reconciliation raw -> intermediate: every distinct event_id that landed in the
-- raw zone exists exactly once after deduplication (nothing lost, nothing doubled).
with raw as (select count(distinct event_id) as n from {{ source('raw', 'trip_events') }}),
     dedup as (select count(*) as n, count(distinct event_id) as n_distinct from {{ ref('int_trip_events') }})
select raw.n as raw_distinct, dedup.n as dedup_rows, dedup.n_distinct
from raw, dedup
where raw.n <> dedup.n or dedup.n <> dedup.n_distinct
