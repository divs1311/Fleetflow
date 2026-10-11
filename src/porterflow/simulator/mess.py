"""Inject the messy reality of mobile event streams into a clean simulated day.

Every injected defect is recorded in a `_gt_*` (ground truth) column that never
leaves the simulator: messages sent to Kafka carry only the payload. Tests and
docs use the ground truth to prove that the pipeline catches what was injected.
"""
from __future__ import annotations

import json
from datetime import date, timedelta

import numpy as np
import pandas as pd

from .trips import DaySimulation
from .world import World

APP_VERSIONS_V2 = ("6.4.0", "6.4.1", "6.5.0")
MALFORMED_KINDS = ("truncated_json", "missing_trip_id", "unknown_city", "bad_timestamp")


def _event_ids(rng: np.random.Generator, n: int) -> list[str]:
    hi = rng.integers(0, 2**63, n, dtype=np.int64)
    lo = rng.integers(0, 2**63, n, dtype=np.int64)
    return [f"{a:016x}{b:016x}" for a, b in zip(hi, lo)]


def _ist_date(ts: pd.Series) -> pd.Series:
    return (ts + pd.Timedelta(minutes=330)).dt.date


def _produced_at(rng, true_ts: pd.Series, late_rate: float):
    n = len(true_ts)
    delay = rng.exponential(3.0, n) + 0.2
    late = rng.random(n) < late_rate
    bucket = rng.random(n)
    late_s = np.where(bucket < 0.7, rng.uniform(1, 6, n),
                      np.where(bucket < 0.9, rng.uniform(6, 24, n), rng.uniform(24, 70, n))) * 3600.0
    delay = np.where(late, late_s, delay)
    return true_ts + pd.to_timedelta(delay, unit="s"), late


def _schema_version(df: pd.DataFrame, cfg, rng) -> None:
    v2 = (_ist_date(df["event_ts_utc"]) >= cfg.schema_v2_from).to_numpy()
    df["schema_version"] = np.where(v2, 2, 1).astype("int16")
    df["app_version"] = np.where(v2, np.array(APP_VERSIONS_V2)[rng.integers(0, 3, len(df))], None)


def _duplicates(rng, df: pd.DataFrame, rate: float) -> pd.DataFrame:
    dup = df[rng.random(len(df)) < rate].copy()
    dup["produced_at_utc"] = dup["produced_at_utc"] + pd.to_timedelta(rng.uniform(1, 600, len(dup)), unit="s")
    dup["_gt_duplicate"] = True
    out = pd.concat([df, dup], ignore_index=True)
    return out.sort_values(["produced_at_utc", "event_id"], kind="stable").reset_index(drop=True)


def inject_trip_mess(world: World, sim: DaySimulation) -> pd.DataFrame:
    cfg = world.cfg
    rng = np.random.default_rng([cfg.seed, sim.day.toordinal(), 2])
    df = sim.trip_events.copy()
    n = len(df)
    df["event_id"] = _event_ids(rng, n)
    true_ts = df["event_ts_utc"].copy()

    # 1. drivers whose app still reports the vehicle they drove before a recent switch
    fleet = world.fleet_as_of(sim.day)
    recent = fleet.drivers[fleet.drivers["previous_vehicle_id"].notna()
                           & (fleet.drivers["vehicle_switched_on"] >= pd.Timestamp(sim.day - timedelta(days=2)))]
    prev_vehicle = dict(zip(recent["driver_id"], recent["previous_vehicle_id"]))
    trips_by_recent = df.loc[df["driver_id"].isin(prev_vehicle.keys()), "trip_id"].unique()
    stale_trips = set(trips_by_recent[rng.random(len(trips_by_recent)) < cfg.stale_vehicle_rate])
    stale = df["trip_id"].isin(stale_trips) & df["driver_id"].notna()
    df.loc[stale, "vehicle_id"] = df.loc[stale, "driver_id"].map(prev_vehicle)
    df["_gt_stale_vehicle"] = stale

    # 2. missing GPS fixes (never on the booking: the customer app geocodes the address)
    null_gps = (df["event_type"] != "trip_booked") & (rng.random(n) < cfg.null_gps_rate)
    df.loc[null_gps, ["lat", "lng"]] = np.nan
    df["_gt_null_gps"] = null_gps

    # 3. odometer/GPS glitch: delivered distance inflated 8-20x -> impossible speed
    glitch = (df["event_type"] == "trip_delivered") & (rng.random(n) < cfg.speed_glitch_rate)
    df.loc[glitch, "distance_km"] = (df.loc[glitch, "distance_km"] * rng.uniform(8, 20, glitch.sum())).round(3)
    df["_gt_speed_glitch"] = glitch

    # 4. wrong device clock: in-trip events stamped 1-20 minutes early
    skew = df["event_type"].isin(["trip_picked_up", "trip_delivered"]) & (rng.random(n) < cfg.clock_skew_rate)
    df.loc[skew, "event_ts_utc"] = df.loc[skew, "event_ts_utc"] - pd.to_timedelta(
        rng.uniform(60, 1200, skew.sum()), unit="s")
    df["_gt_clock_skew"] = skew

    # 5. upload delay (late arrivals) is based on the *true* event time
    df["produced_at_utc"], df["_gt_late"] = _produced_at(rng, true_ts, cfg.late_rate)
    _schema_version(df, cfg, rng)
    df["_gt_duplicate"] = False
    df = _duplicates(rng, df, cfg.duplicate_rate)

    # 6. a handful of messages that violate the contract (go to quarantine)
    bad = rng.random(len(df)) < cfg.malformed_rate
    df["_gt_malformed"] = np.where(bad, np.array(MALFORMED_KINDS)[rng.integers(0, 4, len(df))], None)
    return df


def inject_session_mess(world: World, sim: DaySimulation) -> pd.DataFrame:
    cfg = world.cfg
    rng = np.random.default_rng([cfg.seed, sim.day.toordinal(), 3])
    df = sim.session_events.copy()
    missing = (df["event_type"] == "driver_logout") & (rng.random(len(df)) < cfg.missing_logout_rate)
    df = df[~missing].reset_index(drop=True)
    n = len(df)
    df["event_id"] = _event_ids(rng, n)
    null_gps = rng.random(n) < cfg.null_gps_rate / 2
    df.loc[null_gps, ["lat", "lng"]] = np.nan
    df["_gt_null_gps"] = null_gps
    df["produced_at_utc"], df["_gt_late"] = _produced_at(rng, df["event_ts_utc"], cfg.late_rate / 2)
    _schema_version(df, cfg, rng)
    df["_gt_duplicate"] = False
    df = _duplicates(rng, df, cfg.duplicate_rate)
    df["_gt_malformed"] = None
    return df


# ---------------------------------------------------------------------- serialisation
TRIP_PAYLOAD = ("event_id", "schema_version", "event_type", "event_ts", "produced_at", "trip_id", "city_code",
                "customer_id", "vehicle_type", "driver_id", "vehicle_id", "lat", "lng", "pickup_lat", "pickup_lng",
                "drop_lat", "drop_lng", "estimated_fare", "fare_amount", "distance_km", "payment_mode",
                "cancelled_by", "cancellation_reason", "app_version")
SESSION_PAYLOAD = ("event_id", "schema_version", "event_type", "event_ts", "produced_at", "driver_id", "vehicle_id",
                   "city_code", "lat", "lng", "app_version")


def _iso(ts: pd.Series) -> pd.Series:
    return ts.dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str[:-3] + "Z"


def to_messages(df: pd.DataFrame, kind: str) -> list[tuple[bytes, bytes, str]]:
    """Return (key, value, event_type) tuples ready for Kafka, sparse JSON like a real app."""
    fields = TRIP_PAYLOAD if kind == "trip_events" else SESSION_PAYLOAD
    out = df.copy()
    out["event_ts"] = _iso(out["event_ts_utc"])
    out["produced_at"] = _iso(out["produced_at_utc"])
    key_col = "trip_id" if kind == "trip_events" else "driver_id"
    cols = [c for c in fields if c in out.columns]
    records = out[cols].to_dict("records")
    malformed = out["_gt_malformed"].to_numpy() if "_gt_malformed" in out else [None] * len(out)
    keys = out[key_col].to_numpy()
    messages = []
    for rec, key, bad in zip(records, keys, malformed):
        payload = {}
        for k, v in rec.items():
            if v is None or (isinstance(v, float) and v != v):
                continue
            if isinstance(v, (np.integer,)):
                v = int(v)
            elif isinstance(v, (np.floating,)):
                v = float(v)
            elif isinstance(v, np.str_):
                v = str(v)
            payload[k] = round(v, 6) if isinstance(v, float) and k in ("lat", "lng", "pickup_lat", "pickup_lng",
                                                                         "drop_lat", "drop_lng") else v
        if bad == "missing_trip_id":
            payload.pop("trip_id", None)
        elif bad == "unknown_city":
            payload["city_code"] = "XXX"
        elif bad == "bad_timestamp":
            payload["event_ts"] = "2026-13-45T25:61:00Z"
        value = json.dumps(payload, separators=(",", ":")).encode()
        if bad == "truncated_json":
            value = value[: len(value) // 2]
        messages.append((str(key).encode(), value, rec["event_type"]))
    return messages


def emitted_on(df: pd.DataFrame, ingest_day: date) -> pd.DataFrame:
    """Rows whose upload (produced_at) falls on IST calendar day `ingest_day`."""
    return df[_ist_date(df["produced_at_utc"]) == ingest_day]
