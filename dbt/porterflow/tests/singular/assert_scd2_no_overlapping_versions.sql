-- SCD2 integrity: a driver's versions must not overlap (otherwise the as-of join would
-- duplicate trips) and must be contiguous (no gaps).
with ordered as (
    select
        driver_id,
        valid_from_utc,
        valid_to_utc,
        lead(valid_from_utc) over (partition by driver_id order by valid_from_utc) as next_valid_from
    from {{ ref('dim_driver') }}
)
select *
from ordered
where next_valid_from is not null and next_valid_from <> valid_to_utc
