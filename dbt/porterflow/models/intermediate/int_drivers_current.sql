{{ config(materialized = 'table') }}
-- Latest known state of every driver as of the run date: the input to the SCD2 snapshot.
-- Built from the change log of incremental extracts (only extracts up to run_date are
-- visible, so a backfill of an old day sees the world exactly as it was then).

select * exclude (rn)
from (
    select
        *,
        row_number() over (partition by driver_id order by updated_at_utc desc, extract_date desc) as rn
    from {{ ref('stg_master_drivers') }}
    where extract_date <= {{ run_date() }}
)
where rn = 1
