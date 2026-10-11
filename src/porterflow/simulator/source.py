"""Facade over the simulator used by the producer, the master-data API and tests.

Simulated days are cached as Parquet (deterministic, so the cache is always
safe to reuse). The emission for an ingest day D is everything whose upload time
falls on D: D's on-time events plus events from D-1..D-3 that arrived late.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from .mess import emitted_on, inject_session_mess, inject_trip_mess
from .trips import emission_window, simulate_day
from .world import SimConfig, World


class SimulatorSource:
    def __init__(self, cache_dir: Path, seed: int = 42, trips_per_day: int = 20_000):
        self.world = World(SimConfig(seed=seed, trips_per_day=trips_per_day))
        self.cache_dir = Path(cache_dir) / self.world.cfg.cache_key

    def _paths(self, day: date) -> dict[str, Path]:
        d = self.cache_dir / f"sim_date={day.isoformat()}"
        return {"trip_events": d / "trip_events.parquet", "driver_sessions": d / "driver_sessions.parquet",
                "trips": d / "trips_truth.parquet", "manifest": d / "manifest.json"}

    def day_frames(self, day: date) -> dict[str, pd.DataFrame]:
        paths = self._paths(day)
        if all(p.exists() for p in paths.values()):
            return {k: pd.read_parquet(p) for k, p in paths.items() if k != "manifest"}
        sim = simulate_day(self.world, day)
        frames = {
            "trip_events": inject_trip_mess(self.world, sim),
            "driver_sessions": inject_session_mess(self.world, sim),
            "trips": sim.trips.drop(columns=["driver_idx"]),
        }
        paths["manifest"].parent.mkdir(parents=True, exist_ok=True)
        for k, df in frames.items():
            tmp = paths[k].with_suffix(".tmp")
            df.to_parquet(tmp, index=False)
            tmp.replace(paths[k])
        paths["manifest"].write_text(json.dumps(self.ground_truth(frames, day), indent=2))
        return frames

    @staticmethod
    def ground_truth(frames: dict[str, pd.DataFrame], day: date) -> dict:
        te, ds, trips = frames["trip_events"], frames["driver_sessions"], frames["trips"]
        return {
            "sim_date": day.isoformat(),
            "trips": int(len(trips)),
            "trips_by_status": trips["status"].value_counts().to_dict(),
            "trip_messages": int(len(te)),
            "trip_unique_events": int(te["event_id"].nunique()),
            "session_messages": int(len(ds)),
            "injected": {
                "duplicates": int(te["_gt_duplicate"].sum() + ds["_gt_duplicate"].sum()),
                "late": int(te["_gt_late"].sum() + ds["_gt_late"].sum()),
                "null_gps": int(te["_gt_null_gps"].sum() + ds["_gt_null_gps"].sum()),
                "speed_glitch": int(te["_gt_speed_glitch"].sum()),
                "clock_skew": int(te["_gt_clock_skew"].sum()),
                "stale_vehicle_events": int(te["_gt_stale_vehicle"].sum()),
                "malformed": int(te["_gt_malformed"].notna().sum()),
            },
        }

    def emission(self, ingest_day: date) -> dict[str, pd.DataFrame]:
        """All messages the apps upload during IST day `ingest_day`, in upload order."""
        out: dict[str, list[pd.DataFrame]] = {"trip_events": [], "driver_sessions": []}
        for day in emission_window(ingest_day):
            frames = self.day_frames(day)
            for k in out:
                out[k].append(emitted_on(frames[k], ingest_day))
        return {k: pd.concat(v, ignore_index=True).sort_values("produced_at_utc", kind="stable")
                .reset_index(drop=True) for k, v in out.items()}
