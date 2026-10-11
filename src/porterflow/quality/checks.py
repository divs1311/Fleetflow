"""Custom data-quality checks that sit *around* dbt.

Two gates in the daily DAG:
  * raw gate (before transform): is what we just landed fit to load?
  * warehouse gate (after dbt build, before publish): do the layers reconcile,
    and are the published numbers plausible?

Each check returns PASS / WARN / FAIL. Any FAIL raises DataQualityError, which
fails the Airflow task and stops publishing, so a bad day never reaches the
dashboard. Every result (including PASS) is stored in ops.dq_check_results,
giving a history of data health that the dashboard shows.
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

from ..config import Settings
from ..simulator.world import ist_midnight_utc

log = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    pass


@dataclass
class CheckResult:
    layer: str
    check_name: str
    status: str            # PASS | WARN | FAIL
    observed: float | None
    threshold: str
    details: str


def _status(value: float, warn_above: float | None = None, fail_above: float | None = None,
            warn_below: float | None = None, fail_below: float | None = None) -> str:
    if (fail_above is not None and value > fail_above) or (fail_below is not None and value < fail_below):
        return "FAIL"
    if (warn_above is not None and value > warn_above) or (warn_below is not None and value < warn_below):
        return "WARN"
    return "PASS"


def _glob_has_files(path: Path, pattern: str) -> bool:
    return path.exists() and any(path.glob(pattern))


def _raw_view(con, settings: Settings, table: str) -> bool:
    base = settings.raw_table_dir(table)
    if not _glob_has_files(base, "*/*/*.parquet"):
        return False
    con.execute(f"create or replace view {table} as select * from read_parquet("
                f"'{base}/*/*/*.parquet', hive_partitioning = true, union_by_name = true)")
    return True


# ------------------------------------------------------------------------------------- raw gate
def run_raw_checks(settings: Settings, run_date: date) -> list[CheckResult]:
    manifest_path = settings.run_dir(run_date) / "consume_manifest.json"
    if not manifest_path.exists():
        raise DataQualityError(f"no consume manifest for {run_date}: did the ingest task run?")
    manifest = json.loads(manifest_path.read_text())
    batches = manifest["batches"] or ["<none>"]
    batch_list = ", ".join(f"'{b}'" for b in batches)
    con = duckdb.connect()
    results: list[CheckResult] = []
    has_trips = _raw_view(con, settings, "trip_events")
    has_sessions = _raw_view(con, settings, "driver_sessions")

    landed = manifest["valid"]["trip_events"]
    results.append(CheckResult("raw", "trip_events_landed", "PASS" if landed > 0 else "FAIL", landed, "> 0",
                               f"{landed} valid trip events landed in {len(manifest['batches'])} batch(es)"))
    landed_s = manifest["valid"]["driver_sessions"]
    results.append(CheckResult("raw", "driver_sessions_landed", "PASS" if landed_s > 0 else "FAIL", landed_s, "> 0",
                               f"{landed_s} valid session events landed"))
    total = landed + landed_s + manifest["quarantined"]
    q_rate = manifest["quarantined"] / total if total else 0.0
    results.append(CheckResult("raw", "quarantine_rate", _status(q_rate, warn_above=0.001, fail_above=0.01), q_rate,
                               "warn > 0.1%, fail > 1%", f"{manifest['quarantined']} messages violated the contract"))

    if has_trips and landed > 0:
        run = f"(select * from trip_events where _batch_id in ({batch_list}))"
        nulls = con.execute(f"select count(*) from {run} where event_id is null or trip_id is null "
                            f"or event_ts_utc is null").fetchone()[0]
        results.append(CheckResult("raw", "required_fields_not_null", "PASS" if nulls == 0 else "FAIL", nulls, "= 0",
                                   "event_id, trip_id and event_ts_utc must never be null"))

        dup = con.execute(f"""
            with this_run as {run},
            prior as (select distinct event_id from trip_events where _batch_id not in ({batch_list}))
            select count(*) - count(distinct event_id)
                   + (select count(*) from (select distinct event_id from this_run) t
                      where t.event_id in (select event_id from prior))
            from this_run""").fetchone()[0]
        dup_rate = dup / landed
        results.append(CheckResult("raw", "duplicate_event_rate", _status(dup_rate, warn_above=0.03, fail_above=0.10),
                                   dup_rate, "warn > 3%, fail > 10%",
                                   f"{dup} redelivered events (removed downstream by event_id dedup)"))

        gps_den, gps_null = con.execute(f"select count(*), count(*) filter (where lat is null or lng is null) "
                                        f"from {run} where event_type <> 'trip_booked'").fetchone()
        gps_rate = gps_null / gps_den if gps_den else 0.0
        results.append(CheckResult("raw", "null_gps_rate", _status(gps_rate, warn_above=0.02, fail_above=0.10),
                                   gps_rate, "warn > 2%, fail > 10%", f"{gps_null} of {gps_den} driver-side events"))

        late = con.execute(f"select count(*) filter (where produced_at_utc - event_ts_utc > interval 1 hour) "
                           f"from {run}").fetchone()[0]
        late_rate = late / landed
        results.append(CheckResult("raw", "late_arrival_share", _status(late_rate, warn_above=0.05, fail_above=0.20),
                                   late_rate, "warn > 5%, fail > 20%",
                                   f"{late} events uploaded more than 1h after they happened"))

        future = con.execute(f"select count(*) from {run} "
                             f"where event_ts_utc > produced_at_utc + interval 5 minute").fetchone()[0]
        results.append(CheckResult("raw", "events_from_the_future", _status(future, warn_above=0, fail_above=100),
                                   future, "warn > 0, fail > 100", "event time after upload time (device clock ahead)"))

        speed_den, speed_bad = con.execute(f"""
            with d as (select trip_id, min(event_ts_utc) ts, max(distance_km) km from {run}
                       where event_type = 'trip_delivered' group by 1),
                 p as (select trip_id, min(event_ts_utc) ts from trip_events
                       where event_type = 'trip_picked_up' group by 1)
            select count(*), count(*) filter (where date_diff('second', p.ts, d.ts) > 60
                                              and d.km / (date_diff('second', p.ts, d.ts) / 3600.0) > 100)
            from d join p using (trip_id)""").fetchone()
        speed_rate = speed_bad / speed_den if speed_den else 0.0
        results.append(CheckResult("raw", "impossible_speed_rate", _status(speed_rate, warn_above=0.01,
                                                                            fail_above=0.05),
                                   speed_rate, "warn > 1%, fail > 5%",
                                   f"{speed_bad} deliveries imply > 100 km/h (flagged and excluded from distance KPIs)"))

        versions = con.execute(f"select schema_version, count(*) from {run} group by 1 order by 1").fetchall()
        results.append(CheckResult("raw", "schema_versions_seen", "PASS", len(versions), "known versions only",
                                   ", ".join(f"v{v}: {n}" for v, n in versions)))

    if has_sessions and landed_s > 0:
        # a logout within 16h of the first day the pipeline ran can belong to a shift that
        # started before data collection began: not a defect, so it is excluded
        history_start = ist_midnight_utc(settings.pipeline_start) + timedelta(hours=16)
        unpaired = con.execute(f"""
            select count(*) from (select * from driver_sessions where _batch_id in ({batch_list})) s
            where s.event_type = 'driver_logout' and s.event_ts_utc >= timestamp '{history_start}'
            and not exists (
                select 1 from driver_sessions l where l.event_type = 'driver_login'
                and l.driver_id = s.driver_id and l.event_ts_utc <= s.event_ts_utc
                and l.event_ts_utc > s.event_ts_utc - interval 16 hour)""").fetchone()[0]
        results.append(CheckResult("raw", "logout_without_login", _status(unpaired, warn_above=0, fail_above=500),
                                   unpaired, "warn > 0", "logout events with no login in the previous 16h"))

    drivers_file = settings.master_dir / "drivers" / f"extract_date={run_date.isoformat()}" / "part-0.parquet"
    if drivers_file.exists():
        n, distinct = con.execute(f"select count(*), count(distinct driver_id) from read_parquet('{drivers_file}')"
                                  ).fetchone()
        results.append(CheckResult("raw", "master_driver_ids_unique", "PASS" if n == distinct else "FAIL", n - distinct,
                                   "= 0", f"{n} driver records extracted"))
    else:
        results.append(CheckResult("raw", "master_extract_present", "FAIL", 0, "file exists",
                                   f"missing {drivers_file}"))
    return results


# ------------------------------------------------------------------------------- warehouse gate
def run_warehouse_checks(settings: Settings, run_date: date) -> list[CheckResult]:
    con = duckdb.connect(str(settings.warehouse_path), read_only=True)
    results: list[CheckResult] = []
    raw = settings.raw_table_dir("trip_events")
    raw_distinct = con.execute(f"select count(distinct event_id) from read_parquet('{raw}/*/*/*.parquet', "
                               f"hive_partitioning = true, union_by_name = true)").fetchone()[0]
    dedup = con.execute("select count(*) from intermediate.int_trip_events").fetchone()[0]
    results.append(CheckResult("warehouse", "raw_vs_dedup_event_count", "PASS" if raw_distinct == dedup else "FAIL",
                               dedup - raw_distinct, "difference = 0",
                               f"raw distinct event_ids {raw_distinct:,} vs int_trip_events {dedup:,}"))

    sraw = settings.raw_table_dir("driver_sessions")
    s_distinct = con.execute(f"select count(distinct event_id) from read_parquet('{sraw}/*/*/*.parquet', "
                             f"hive_partitioning = true, union_by_name = true)").fetchone()[0]
    s_dedup = con.execute("select count(*) from intermediate.int_driver_sessions").fetchone()[0]
    results.append(CheckResult("warehouse", "raw_vs_dedup_session_count", "PASS" if s_distinct == s_dedup else "FAIL",
                               s_dedup - s_distinct, "difference = 0",
                               f"raw distinct {s_distinct:,} vs int_driver_sessions {s_dedup:,}"))

    trips_in_events, fact_rows = con.execute(
        "select (select count(distinct trip_id) from intermediate.int_trip_events), "
        "(select count(*) from marts.fct_trips)").fetchone()
    results.append(CheckResult("warehouse", "dedup_vs_fact_trip_count",
                               "PASS" if trips_in_events == fact_rows else "FAIL", fact_rows - trips_in_events,
                               "difference = 0", f"distinct trips {trips_in_events:,} vs fct_trips {fact_rows:,}"))

    max_day = con.execute("select max(booked_date_ist) from marts.fct_trips").fetchone()[0]
    results.append(CheckResult("warehouse", "fact_freshness", "PASS" if max_day and max_day >= run_date else "FAIL",
                               None, f">= {run_date}", f"latest booked_date_ist = {max_day}"))

    day = run_date.isoformat()
    today_trips = con.execute(f"select count(*) from marts.fct_trips where booked_date_ist = date '{day}'").fetchone()[0]
    hist = [r[0] for r in con.execute(
        f"select count(*) from marts.fct_trips where booked_date_ist between date '{day}' - interval 7 day "
        f"and date '{day}' - interval 1 day and booked_date_ist >= date '{settings.pipeline_start}' "
        f"group by booked_date_ist").fetchall()]
    if len(hist) >= 3:
        median = sorted(hist)[len(hist) // 2]
        ratio = today_trips / median if median else 0.0
        status = _status(ratio, warn_above=1.5, fail_above=2.5, warn_below=0.6, fail_below=0.3)
        results.append(CheckResult("warehouse", "daily_volume_vs_7d_median", status, ratio,
                                   "warn outside 0.6-1.5x, fail outside 0.3-2.5x",
                                   f"{today_trips:,} trips vs 7-day median {median:,}"))
    else:
        results.append(CheckResult("warehouse", "daily_volume_vs_7d_median", "PASS" if today_trips > 0 else "FAIL",
                                   today_trips, "> 0 (not enough history for anomaly check)",
                                   f"{today_trips:,} trips booked"))

    rate = con.execute(f"select sum(trips_completed) / nullif(sum(trips_completed + trips_cancelled), 0) "
                       f"from marts.agg_city_daily where booked_date_ist = date '{day}'").fetchone()[0] or 0.0
    results.append(CheckResult("warehouse", "completion_rate_plausible",
                               _status(rate, warn_below=0.6, fail_below=0.3, warn_above=0.97), rate,
                               "warn outside 60-97%, fail < 30%", f"network completion rate {rate:.1%}"))

    agg_sum = con.execute(f"select coalesce(sum(trips_booked), 0) from marts.agg_city_daily "
                          f"where booked_date_ist = date '{day}'").fetchone()[0]
    results.append(CheckResult("warehouse", "kpi_reconciles_with_fact", "PASS" if agg_sum == today_trips else "FAIL",
                               agg_sum - today_trips, "difference = 0",
                               f"agg_city_daily {agg_sum:,} vs fct_trips {today_trips:,}"))

    util_rows, util_bad = con.execute(
        f"select count(*), count(*) filter (where utilisation < 0 or utilisation > 1) "
        f"from marts.fct_driver_hourly_utilisation where activity_date_ist = date '{day}'").fetchone()
    results.append(CheckResult("warehouse", "utilisation_output_valid",
                               "PASS" if util_rows > 0 and util_bad == 0 else "FAIL", util_bad,
                               "rows > 0 and 0 <= utilisation <= 1", f"{util_rows:,} driver-hours for {day}"))

    assigned, unmatched = con.execute(
        "select count(*), count(*) filter (where driver_sk is null) from marts.fct_trips "
        "where driver_id is not null").fetchone()
    um_rate = unmatched / assigned if assigned else 0.0
    results.append(CheckResult("warehouse", "trips_without_driver_version",
                               _status(um_rate, warn_above=0.001, fail_above=0.02), um_rate,
                               "warn > 0.1%, fail > 2%", f"{unmatched} assigned trips with no SCD2 driver match"))

    stuck = con.execute(f"select count(*) filter (where trip_status = 'in_progress'), count(*) from marts.fct_trips "
                        f"where booked_date_ist < date '{day}' - interval 1 day "
                        f"and booked_date_ist >= date '{settings.pipeline_start}'").fetchone()
    stuck_rate = stuck[0] / stuck[1] if stuck[1] else 0.0
    results.append(CheckResult("warehouse", "stale_in_progress_trips",
                               _status(stuck_rate, warn_above=0.005, fail_above=0.05), stuck_rate,
                               "warn > 0.5%, fail > 5%", f"{stuck[0]} trips older than a day still in progress"))

    mismatch = con.execute(f"select count(*) filter (where is_vehicle_mismatch), count(*) from marts.fct_trips "
                           f"where booked_date_ist = date '{day}' and driver_id is not null").fetchone()
    mm_rate = mismatch[0] / mismatch[1] if mismatch[1] else 0.0
    results.append(CheckResult("warehouse", "vehicle_mismatch_rate", _status(mm_rate, warn_above=0.005,
                                                                             fail_above=0.05), mm_rate,
                               "warn > 0.5%, fail > 5%",
                               f"{mismatch[0]} trips where the app's vehicle differs from master data"))
    con.close()
    return results


# ------------------------------------------------------------------------------------ persistence
def persist(settings: Settings, run_date: date, layer: str, results: list[CheckResult]) -> None:
    checked_at = datetime.now(timezone.utc).replace(tzinfo=None)
    out = settings.quality_dir / f"run_date={run_date.isoformat()}" / f"{layer}_checks.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps([asdict(r) for r in results], indent=2, default=str))
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(settings.warehouse_path))
    try:
        con.execute("create schema if not exists ops")
        con.execute("""create table if not exists ops.dq_check_results (
            run_date date, layer varchar, check_name varchar, status varchar, observed double,
            threshold varchar, details varchar, checked_at_utc timestamp)""")
        con.execute("begin transaction")
        con.execute("delete from ops.dq_check_results where run_date = ? and layer = ?", [run_date, layer])
        con.executemany("insert into ops.dq_check_results values (?, ?, ?, ?, ?, ?, ?, ?)",
                        [[run_date, r.layer, r.check_name, r.status,
                          None if r.observed is None else float(r.observed), r.threshold, r.details, checked_at]
                         for r in results])
        con.execute("commit")
    finally:
        con.close()


def gate(settings: Settings, run_date: date, layer: str) -> list[CheckResult]:
    results = run_raw_checks(settings, run_date) if layer == "raw" else run_warehouse_checks(settings, run_date)
    persist(settings, run_date, layer, results)
    for r in results:
        log.log(logging.ERROR if r.status == "FAIL" else logging.WARNING if r.status == "WARN" else logging.INFO,
                "[%s] %-32s %-4s observed=%s (%s) %s", layer, r.check_name, r.status, r.observed, r.threshold,
                r.details)
    failed = [r.check_name for r in results if r.status == "FAIL"]
    if failed:
        raise DataQualityError(f"{layer} quality gate failed for {run_date}: {failed}")
    return results
