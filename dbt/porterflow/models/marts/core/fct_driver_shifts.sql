-- Grain: one row per driver login (shift). Logout = the next session event if it is a logout.
-- A missing logout (app killed, or shift still running at the data cut-off) is capped at
-- 14 hours or the latest event seen, and flagged, instead of silently inflating online time.
with events as (
    select driver_id, city_code, vehicle_id, event_type, event_ts_utc
    from {{ ref('int_driver_sessions') }}
),

sequenced as (
    select
        *,
        lead(event_type)   over w as next_event_type,
        lead(event_ts_utc) over w as next_event_ts_utc
    from events
    window w as (partition by driver_id order by event_ts_utc, event_type)
),

data_cutoff as (
    select max(event_ts_utc) as max_ts from events
)

select
    md5(s.driver_id || '|' || cast(s.event_ts_utc as varchar))                     as shift_id,
    s.driver_id,
    s.city_code,
    s.vehicle_id,
    cast({{ to_ist('s.event_ts_utc') }} as date)                                    as shift_date_ist,
    s.event_ts_utc                                                                  as login_at_utc,
    case when s.next_event_type = 'driver_logout' then s.next_event_ts_utc end      as logout_at_utc,
    coalesce(s.next_event_type <> 'driver_logout', true)                            as is_logout_missing,
    date_diff(
        'second',
        s.event_ts_utc,
        case when s.next_event_type = 'driver_logout' then s.next_event_ts_utc
             else least(s.event_ts_utc + interval 14 hour, c.max_ts) end
    ) / 60.0                                                                        as online_minutes
from sequenced as s
cross join data_cutoff as c
where s.event_type = 'driver_login'
