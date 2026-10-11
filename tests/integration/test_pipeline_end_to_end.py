"""End-to-end: simulator -> (in-memory Kafka) -> raw lake -> quality gate -> Spark -> dbt -> gate -> serving.

Proves the properties that matter for a data engineer, against the simulator's ground truth:
  * late events are absorbed (trips move from in_progress to their final status),
  * re-running a day changes nothing (idempotency),
  * the SCD2 driver history is built and used by the fact table,
  * final trip statuses match what really happened in the simulation.
Needs a JRE (Spark) and dbt; skipped otherwise. Run with: pytest -m integration
"""
from __future__ import annotations

import os
import shutil
from datetime import date, timedelta

import duckdb
import pandas as pd
import pytest

from kafka_fakes import FakeConsumer, FakeLog, emit_day, land
from porterflow import tasks
from porterflow.api import app as api_module
from porterflow.ingestion import master_data
from porterflow.simulator import SimConfig, SimulatorSource, World
from api_fakes import FlakyThenOK

DBT = os.environ.get("DBT_EXECUTABLE") or shutil.which("dbt")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not DBT, reason="dbt executable not found (set DBT_EXECUTABLE)"),
    pytest.mark.skipif(not (os.environ.get("JAVA_HOME") or shutil.which("java")), reason="Spark needs a JRE"),
]

START = date(2026, 9, 1)
DAYS = 4


def _fct(settings) -> pd.DataFrame:
    con = duckdb.connect(str(settings.serving_path), read_only=True)
    try:
        return con.execute("select * from marts.fct_trips").df()
    finally:
        con.close()


def _run_day(settings, log, consumer, day, emit=True):
    if emit:
        emit_day(log, settings, day)
    land(log, consumer, settings, day)
    master_data.extract_master_data(settings, day)
    tasks.raw_quality_gate(settings, day)
    tasks.spark_utilisation(settings, day)
    tasks.dbt(settings, day, extra=["--quiet"])
    tasks.warehouse_quality_gate(settings, day)
    tasks.publish(settings, day)


@pytest.fixture(scope="module")
def pipeline(tmp_path_factory):
    mp = pytest.MonkeyPatch()
    data_dir = tmp_path_factory.mktemp("e2e") / "data"
    for k, v in {"PORTERFLOW_DATA_DIR": str(data_dir), "SIM_TRIPS_PER_DAY": "1500", "SIM_SEED": "42",
                 "PORTERFLOW_START_DATE": START.isoformat(), "DBT_EXECUTABLE": DBT,
                 "PORTERFLOW_EXPORT_TARGET": "none", "SPARK_MASTER": "local[2]"}.items():
        mp.setenv(k, v)
    mp.setattr(api_module, "world", World(SimConfig(seed=42, trips_per_day=1500)))
    mp.setattr(master_data.requests, "Session", lambda: FlakyThenOK(fail_times=1))
    mp.setattr(master_data.time, "sleep", lambda s: None)
    from porterflow.config import load_settings
    settings = load_settings()
    log, consumer = FakeLog(settings), FakeConsumer()

    snapshots, offsets_before = {}, {}
    for k in range(DAYS):
        day = START + timedelta(days=k)
        offsets_before[day] = dict(consumer.committed)
        _run_day(settings, log, consumer, day)
        snapshots[day] = _fct(settings)
    yield settings, log, consumer, snapshots, offsets_before
    mp.undo()


def test_late_events_are_absorbed(pipeline):
    _, _, _, snaps, _ = pipeline
    day1 = START
    after_day1 = snaps[day1].set_index("trip_id")
    final = snaps[START + timedelta(days=DAYS - 1)].set_index("trip_id")
    booked_day1 = pd.to_datetime(after_day1.booked_date_ist).dt.date == day1
    was_open = after_day1[(after_day1.trip_status == "in_progress") & booked_day1].index
    assert len(was_open) > 0
    settled = final.loc[was_open, "trip_status"].isin(["completed", "cancelled"]).mean()
    assert settled > 0.95
    assert final.index.is_unique  # accumulating snapshot: still one row per trip


def test_final_statuses_match_simulator_truth(pipeline):
    settings, _, _, snaps, _ = pipeline
    src = SimulatorSource(settings.state_dir / "sim_cache", 42, 1500)
    truth = src.day_frames(START)["trips"][["trip_id", "status"]]
    fct = snaps[START + timedelta(days=DAYS - 1)][["trip_id", "trip_status"]]
    joined = truth.merge(fct, on="trip_id", how="left")
    agreement = (joined.status == joined.trip_status).mean()
    assert agreement > 0.995, joined[joined.status != joined.trip_status].head()


def test_scd2_history_is_used_by_the_fact(pipeline):
    settings, *_ = pipeline
    con = duckdb.connect(str(settings.serving_path), read_only=True)
    versions = con.execute("select count(*) from marts.dim_driver where version_number > 1").fetchone()[0]
    assert versions > 0
    unmatched = con.execute("select count(*) from marts.fct_trips where driver_id is not null and driver_sk is null"
                            ).fetchone()[0]
    wrong_version = con.execute("""
        select count(*) from marts.fct_trips f join marts.dim_driver d using (driver_sk)
        where not (f.assigned_at_utc >= d.valid_from_utc and f.assigned_at_utc < d.valid_to_utc)""").fetchone()[0]
    con.close()
    assert unmatched == 0 and wrong_version == 0


def test_rerunning_a_day_is_idempotent(pipeline):
    settings, log, consumer, snaps, offsets_before = pipeline
    last = START + timedelta(days=DAYS - 1)
    before = snaps[last]
    # simulate a crash after the last day's files were written but before its offsets were
    # committed: the consumer restarts from the previous commit and the whole day is re-run
    consumer.committed = dict(offsets_before[last])
    _run_day(settings, log, consumer, last, emit=False)
    after = _fct(settings)
    assert len(after) == len(before)
    pd.testing.assert_series_equal(after.groupby("trip_status").size(), before.groupby("trip_status").size())
    assert after.fare_amount.sum() == pytest.approx(before.fare_amount.sum())


def test_quality_history_recorded(pipeline):
    settings, *_ = pipeline
    con = duckdb.connect(str(settings.serving_path), read_only=True)
    res = con.execute("select layer, count(distinct run_date), count(*) filter (where status = 'FAIL') "
                      "from ops.dq_check_results group by 1 order by 1").fetchall()
    con.close()
    assert res == [("raw", DAYS, 0), ("warehouse", DAYS, 0)]
