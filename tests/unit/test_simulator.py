from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from porterflow.schemas import ContractViolation, decode_json, validate_session_event, validate_trip_event
from porterflow.simulator import SimConfig, SimulatorSource, World
from porterflow.simulator.mess import to_messages
from porterflow.simulator.world import ist_midnight_utc

DAY = date(2026, 9, 3)


@pytest.fixture(scope="module")
def source(tmp_path_factory):
    return SimulatorSource(tmp_path_factory.mktemp("sim"), seed=7, trips_per_day=1500)


def test_simulation_is_deterministic(tmp_path):
    a = SimulatorSource(tmp_path / "a", seed=7, trips_per_day=800).day_frames(DAY)
    b = SimulatorSource(tmp_path / "b", seed=7, trips_per_day=800).day_frames(DAY)
    pd.testing.assert_frame_equal(a["trip_events"], b["trip_events"])
    pd.testing.assert_frame_equal(a["driver_sessions"], b["driver_sessions"])


def test_cache_returns_identical_frames(source):
    first = source.day_frames(DAY)["trip_events"]
    again = source.day_frames(DAY)["trip_events"]  # read back from the Parquet cache
    pd.testing.assert_frame_equal(first.reset_index(drop=True), again.reset_index(drop=True), check_dtype=False)


def test_trip_lifecycle_is_consistent(source):
    trips = source.day_frames(DAY)["trips"]
    done = trips[trips.status == "completed"]
    assert (done.booked_s <= done.assign_s).all()
    assert (done.assign_s < done.picked_s).all() and (done.picked_s < done.delivered_s).all()
    assert trips.loc[trips.status == "cancelled", "cancellation_reason"].notna().all()
    assert 0.45 < (trips.status == "completed").mean() < 0.95


def test_injected_defects_are_recorded(source):
    te = source.day_frames(DAY)["trip_events"]
    dups = te[te._gt_duplicate]
    assert len(dups) > 0 and dups.event_id.isin(te[~te._gt_duplicate].event_id).all()
    late = te[te._gt_late]
    assert ((late.produced_at_utc - late.event_ts_utc) >= pd.Timedelta(hours=1)).all()
    assert te.loc[te._gt_null_gps, "lat"].isna().all()
    assert (te.loc[te.event_type == "trip_booked", "lat"].notna()).all()


def test_every_message_matches_the_contract_except_the_injected_bad_ones(source):
    em = source.emission(DAY)
    msgs = to_messages(em["trip_events"], "trip_events")
    bad = 0
    for _, value, _ in msgs:
        try:
            validate_trip_event(decode_json(value))
        except ContractViolation:
            bad += 1
    assert bad == em["trip_events"]["_gt_malformed"].notna().sum()
    for _, value, _ in to_messages(em["driver_sessions"], "driver_sessions"):
        validate_session_event(decode_json(value))


def test_emission_includes_late_events_from_earlier_days(source):
    em = source.emission(DAY)["trip_events"]
    ist_day = (em.produced_at_utc + pd.Timedelta(minutes=330)).dt.date
    assert (ist_day == DAY).all()
    event_day = (em.event_ts_utc + pd.Timedelta(minutes=330)).dt.date
    assert (event_day < DAY).any()


def test_fleet_changes_drive_scd2_history():
    world = World(SimConfig(seed=7, trips_per_day=1500))
    switched_total = 0
    for k in range(10):
        day = DAY + timedelta(days=k)
        before, after = world.fleet_as_of(day - timedelta(days=1)), world.fleet_as_of(day)
        merged = before.drivers.merge(after.drivers, on="driver_id", suffixes=("_b", "_a"))
        switched = merged[merged.vehicle_id_b != merged.vehicle_id_a]
        assert (switched.updated_at_utc_a >= ist_midnight_utc(day)).all()   # change stamped on its day
        assert (switched.previous_vehicle_id_a == switched.vehicle_id_b).all()
        assert len(after.drivers) >= len(before.drivers)                     # onboarding only adds
        switched_total += len(switched)
    assert switched_total > 0


def test_fleet_before_an_instant_is_consistent_with_full_day():
    world = World(SimConfig(seed=7, trips_per_day=1500))
    end_of_day = ist_midnight_utc(DAY + timedelta(days=1))
    full = world.fleet_as_of(DAY).drivers.sort_values("driver_id").reset_index(drop=True)
    via_instant = world.fleet_before(end_of_day).drivers.sort_values("driver_id").reset_index(drop=True)
    pd.testing.assert_frame_equal(full, via_instant, check_dtype=False)
    start = world.fleet_before(ist_midnight_utc(DAY)).drivers
    assert set(start.driver_id) == set(world.fleet_as_of(DAY - timedelta(days=1)).drivers.driver_id)
    assert (start.updated_at_utc < ist_midnight_utc(DAY)).all()
    assert isinstance(start.updated_at_utc.iloc[0], (pd.Timestamp, datetime))
