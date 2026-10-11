{{ config(materialized = 'table') }}
-- Latest state of every vehicle (vehicles are SCD Type 1: attributes are overwritten).

select * exclude (rn)
from (
    select
        *,
        row_number() over (partition by vehicle_id order by updated_at_utc desc, extract_date desc) as rn
    from {{ ref('stg_master_vehicles') }}
    where extract_date <= {{ run_date() }}
)
where rn = 1
