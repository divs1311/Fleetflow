"""The simulated 'real world': cities, vehicle types and a fleet that changes over time.

Everything is deterministic for a given seed: the fleet on any date (and the
master-data API built on top of it) can be recomputed exactly, which is what
makes backfills reproducible. Daily fleet changes happen between 00:30 and
05:30 IST, before shifts start, so a driver uses one vehicle for a whole day:
  * ~0.4% of active drivers switch vehicles each day   -> SCD Type 2 history
  * ~0.12% of active drivers are deactivated each day
  * new drivers (with new vehicles) are onboarded every day
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from functools import lru_cache

import numpy as np
import pandas as pd

IST_OFFSET = timedelta(minutes=330)


@dataclass(frozen=True)
class City:
    code: str
    name: str
    state: str
    lat: float
    lng: float
    radius_km: float
    demand_weight: float
    rto_prefix: str


CITIES: dict[str, City] = {
    c.code: c
    for c in [
        City("BLR", "Bengaluru", "Karnataka", 12.9716, 77.5946, 17.0, 0.24, "KA"),
        City("MUM", "Mumbai", "Maharashtra", 19.0760, 72.8777, 18.0, 0.22, "MH"),
        City("DEL", "Delhi NCR", "Delhi", 28.6139, 77.2090, 24.0, 0.22, "DL"),
        City("HYD", "Hyderabad", "Telangana", 17.3850, 78.4867, 17.0, 0.13, "TS"),
        City("PUN", "Pune", "Maharashtra", 18.5204, 73.8567, 13.0, 0.10, "MH"),
        City("CHE", "Chennai", "Tamil Nadu", 13.0827, 80.2707, 15.0, 0.09, "TN"),
    ]
}


@dataclass(frozen=True)
class VehicleSpec:
    code: str
    demand_share: float
    capacity_kg: int
    base_fare: float
    per_km: float
    speed_kmph: float
    median_trip_km: float
    load_minutes: tuple[float, float]
    max_approach_km: float
    trips_per_shift: float
    makes: tuple[str, ...]
    fuels: tuple[str, ...]


VEHICLE_SPECS: dict[str, VehicleSpec] = {
    v.code: v
    for v in [
        VehicleSpec("two_wheeler", 0.42, 20, 45, 9, 24, 6.0, (2, 6), 8.0, 10.0,
                    ("Honda Activa", "TVS Jupiter", "Hero Splendor", "Ather 450X"), ("petrol", "petrol", "electric")),
        VehicleSpec("three_wheeler", 0.18, 500, 120, 16, 18, 7.0, (5, 12), 9.0, 7.0,
                    ("Bajaj RE", "Piaggio Ape", "Mahindra Treo"), ("cng", "cng", "electric")),
        VehicleSpec("tata_ace", 0.25, 750, 250, 22, 20, 9.0, (10, 22), 11.0, 6.0,
                    ("Tata Ace Gold", "Tata Ace EV", "Maruti Super Carry"), ("diesel", "cng", "electric")),
        VehicleSpec("pickup_8ft", 0.10, 1250, 400, 28, 19, 11.0, (12, 28), 13.0, 5.0,
                    ("Mahindra Bolero Pickup", "Ashok Leyland Dost", "Tata Intra V30"), ("diesel", "cng")),
        VehicleSpec("truck_14ft", 0.05, 3500, 900, 45, 16, 16.0, (20, 40), 15.0, 4.0,
                    ("Tata 407", "Eicher Pro 2049", "Ashok Leyland Partner"), ("diesel",)),
    ]
}
VEHICLE_ORDER = list(VEHICLE_SPECS)

SHIFT_TYPES = ("morning", "day", "evening", "night")
SHIFT_SHARES = (0.33, 0.27, 0.32, 0.08)

FIRST_NAMES = ("Aarav", "Vivek", "Rahul", "Suresh", "Ramesh", "Imran", "Arjun", "Karthik", "Manoj", "Sanjay",
               "Deepak", "Anil", "Ravi", "Prakash", "Mohammed", "Sunil", "Ajay", "Vijay", "Naveen", "Harish",
               "Lakshmi", "Priya", "Pooja", "Kavya", "Sneha", "Farhan", "Gurpreet", "Joseph", "Ashok", "Dinesh")
LAST_NAMES = ("Kumar", "Sharma", "Singh", "Reddy", "Patil", "Khan", "Nair", "Iyer", "Yadav", "Gupta", "Das",
              "Shaikh", "Rao", "Pillai", "Verma", "Joshi", "Naidu", "Gowda", "Mishra", "Chauhan")


@dataclass(frozen=True)
class SimConfig:
    seed: int = 42
    trips_per_day: int = 20_000
    world_start: date = date(2026, 7, 1)
    # injected "messy reality" (rates are per event unless noted)
    duplicate_rate: float = 0.015         # at-least-once redelivery from the app/SDK
    late_rate: float = 0.02               # events stuck on the phone, uploaded 1-72 h later
    null_gps_rate: float = 0.01           # GPS fix missing (tunnels, basements)
    speed_glitch_rate: float = 0.003      # odometer/GPS glitch on delivered events
    clock_skew_rate: float = 0.005        # device clock wrong -> out-of-order timestamps
    stale_vehicle_rate: float = 0.35      # share of recently switched drivers whose app still reports the old vehicle
    malformed_rate: float = 0.0003        # corrupted / contract-violating messages
    missing_logout_rate: float = 0.005    # driver app killed before logout
    schema_v2_from: date = date(2026, 9, 15)
    rain_events: tuple[tuple[date, str], ...] = ((date(2026, 9, 10), "MUM"), (date(2026, 9, 23), "BLR"))

    @property
    def cache_key(self) -> str:
        return f"s{self.seed}-t{self.trips_per_day}-v3"


@dataclass
class FleetState:
    """Master data as of a moment in time (one row per driver / per vehicle)."""
    drivers: pd.DataFrame
    vehicles: pd.DataFrame
    next_driver_seq: dict[str, int] = field(default_factory=dict)
    next_vehicle_seq: dict[str, int] = field(default_factory=dict)

    def copy(self) -> "FleetState":
        return FleetState(self.drivers.copy(), self.vehicles.copy(),
                          dict(self.next_driver_seq), dict(self.next_vehicle_seq))


def ist_midnight_utc(day: date) -> datetime:
    """UTC instant (naive) of 00:00 IST on `day`."""
    return datetime(day.year, day.month, day.day) - IST_OFFSET


def random_point(rng: np.random.Generator, lat: float, lng: float, radius_km: float, size: int):
    """Uniform points in a disc (good enough at city scale)."""
    r = radius_km * np.sqrt(rng.random(size))
    theta = rng.random(size) * 2 * math.pi
    dlat = (r * np.cos(theta)) / 111.0
    dlng = (r * np.sin(theta)) / (111.0 * math.cos(math.radians(lat)))
    return lat + dlat, lng + dlng


class World:
    def __init__(self, cfg: SimConfig | None = None):
        self.cfg = cfg or SimConfig()
        self._day_states: dict[date, FleetState] = {}
        self._base = self._initial_fleet()

    # ------------------------------------------------------------------ fleet
    def base_fleet_size(self, city: str, vtype: str) -> int:
        spec, c = VEHICLE_SPECS[vtype], CITIES[city]
        demand = self.cfg.trips_per_day * c.demand_weight * spec.demand_share
        return max(3, int(math.ceil(demand / spec.trips_per_shift / 0.85 * 1.5)))

    def _new_vehicle_rows(self, rng, city: str, vtype: str, seqs: list[int], created: list[datetime]) -> pd.DataFrame:
        spec, c = VEHICLE_SPECS[vtype], CITIES[city]
        n = len(seqs)
        regs = [f"{c.rto_prefix}{rng.integers(1, 60):02d}{chr(65 + rng.integers(0, 26))}{chr(65 + rng.integers(0, 26))}"
                f"{rng.integers(1000, 10000)}" for _ in range(n)]
        return pd.DataFrame({
            "vehicle_id": [f"VEH{city}{s:06d}" for s in seqs],
            "registration_no": regs,
            "vehicle_type": vtype,
            "city_code": city,
            "fuel_type": [spec.fuels[i] for i in rng.integers(0, len(spec.fuels), n)],
            "make_model": [spec.makes[i] for i in rng.integers(0, len(spec.makes), n)],
            "capacity_kg": spec.capacity_kg,
            "created_at_utc": created,
            "updated_at_utc": created,
        })

    def _new_driver_rows(self, rng, city: str, vtype: str, seqs: list[int], vehicle_ids: list[str],
                         onboarded: list[datetime]) -> pd.DataFrame:
        c = CITIES[city]
        n = len(seqs)
        home_lat, home_lng = random_point(rng, c.lat, c.lng, c.radius_km, n)
        return pd.DataFrame({
            "driver_id": [f"DRV{city}{s:06d}" for s in seqs],
            "full_name": [f"{FIRST_NAMES[a]} {LAST_NAMES[b]}" for a, b in
                          zip(rng.integers(0, len(FIRST_NAMES), n), rng.integers(0, len(LAST_NAMES), n))],
            "phone_masked": [f"+91 9{rng.integers(0, 10)}XXXXX{rng.integers(100, 1000)}" for _ in range(n)],
            "city_code": city,
            "vehicle_id": vehicle_ids,
            "vehicle_type": vtype,
            "shift_type": rng.choice(SHIFT_TYPES, size=n, p=SHIFT_SHARES),
            "status": "active",
            "rating": np.round(rng.normal(4.55, 0.2, n).clip(3.5, 5.0), 2),
            "home_lat": home_lat,
            "home_lng": home_lng,
            "onboarded_at_utc": onboarded,
            "updated_at_utc": onboarded,
            "previous_vehicle_id": None,
            "vehicle_switched_on": pd.NaT,
        })

    def _initial_fleet(self) -> FleetState:
        rng = np.random.default_rng([self.cfg.seed, 0, 99])
        drivers, vehicles = [], []
        dseq: dict[str, int] = {}
        vseq: dict[str, int] = {}
        span_s = int((ist_midnight_utc(self.cfg.world_start) - datetime(2025, 1, 1)).total_seconds())
        for city in CITIES:
            dseq[city] = vseq[city] = 1
            for vtype in VEHICLE_SPECS:
                n = self.base_fleet_size(city, vtype)
                when = [datetime(2025, 1, 1) + timedelta(seconds=int(s)) for s in rng.integers(0, span_s, n)]
                vs = list(range(vseq[city], vseq[city] + n))
                ds = list(range(dseq[city], dseq[city] + n))
                vseq[city] += n
                dseq[city] += n
                v = self._new_vehicle_rows(rng, city, vtype, vs, when)
                vehicles.append(v)
                drivers.append(self._new_driver_rows(rng, city, vtype, ds, list(v["vehicle_id"]), when))
        return FleetState(pd.concat(drivers, ignore_index=True), pd.concat(vehicles, ignore_index=True), dseq, vseq)

    def _apply_day_changes(self, state: FleetState, day: date) -> FleetState:
        """Apply the (deterministic) master-data changes that happen on `day`."""
        rng = np.random.default_rng([self.cfg.seed, day.toordinal(), 11])
        s = state.copy()
        d = s.drivers
        midnight = ist_midnight_utc(day)

        def change_times(n: int) -> list[datetime]:
            return [midnight + timedelta(seconds=int(x)) for x in rng.integers(1800, 19800, n)]

        active_idx = np.flatnonzero((d["status"] == "active").to_numpy())
        switch_mask = rng.random(len(active_idx)) < 0.004
        deact_mask = (rng.random(len(active_idx)) < 0.0012) & ~switch_mask
        switch_idx, deact_idx = active_idx[switch_mask], active_idx[deact_mask]
        switch_times, deact_times = change_times(len(switch_idx)), change_times(len(deact_idx))
        upgrade_draw = rng.random(len(switch_idx))
        new_vehicle_frames = []
        applied_switch = []
        for i, idx in enumerate(switch_idx):
            city = d.at[idx, "city_code"]
            old_type = d.at[idx, "vehicle_type"]
            pos = VEHICLE_ORDER.index(old_type)
            if upgrade_draw[i] < 0.12 and pos < len(VEHICLE_ORDER) - 1:
                new_type = VEHICLE_ORDER[pos + 1]
            elif upgrade_draw[i] > 0.94 and pos > 0:
                new_type = VEHICLE_ORDER[pos - 1]
            else:
                new_type = old_type
            seq = s.next_vehicle_seq[city]
            s.next_vehicle_seq[city] += 1
            v = self._new_vehicle_rows(rng, city, new_type, [seq], [switch_times[i]])
            new_vehicle_frames.append(v)
            applied_switch.append((idx, v["vehicle_id"].iat[0], new_type, switch_times[i]))
        for idx, vid, vtype, ts in applied_switch:
            d.at[idx, "previous_vehicle_id"] = d.at[idx, "vehicle_id"]
            d.at[idx, "vehicle_switched_on"] = pd.Timestamp(day)
            d.at[idx, "vehicle_id"] = vid
            d.at[idx, "vehicle_type"] = vtype
            d.at[idx, "updated_at_utc"] = ts
        for i, idx in enumerate(deact_idx):
            d.at[idx, "status"] = "inactive"
            d.at[idx, "updated_at_utc"] = deact_times[i]

        onboard_frames, onboard_vehicles = [], []
        for city in CITIES:
            for vtype in VEHICLE_SPECS:
                k = int(rng.poisson(self.base_fleet_size(city, vtype) * 0.0016))
                if k == 0:
                    continue
                keep = change_times(k)
                vs = list(range(s.next_vehicle_seq[city], s.next_vehicle_seq[city] + len(keep)))
                ds = list(range(s.next_driver_seq[city], s.next_driver_seq[city] + len(keep)))
                s.next_vehicle_seq[city] += len(keep)
                s.next_driver_seq[city] += len(keep)
                v = self._new_vehicle_rows(rng, city, vtype, vs, keep)
                onboard_vehicles.append(v)
                onboard_frames.append(self._new_driver_rows(rng, city, vtype, ds, list(v["vehicle_id"]), keep))
        s.drivers = pd.concat([d, *onboard_frames], ignore_index=True)
        s.vehicles = pd.concat([s.vehicles, *new_vehicle_frames, *onboard_vehicles], ignore_index=True)
        return s

    def fleet_as_of(self, day: date) -> FleetState:
        """Fleet after all changes of `day` (i.e. the fleet that drives on `day`)."""
        if day < self.cfg.world_start:
            return self._base
        if day in self._day_states:
            return self._day_states[day]
        known = [d for d in self._day_states if d < day]
        cur_day = max(known) if known else self.cfg.world_start - timedelta(days=1)
        state = self._day_states.get(cur_day, self._base)
        while cur_day < day:
            cur_day += timedelta(days=1)
            state = self._apply_day_changes(state, cur_day)
            self._day_states[cur_day] = state
        return state

    @lru_cache(maxsize=64)
    def fleet_before(self, instant_utc: datetime) -> FleetState:
        """Fleet including only the changes that happened strictly before `instant_utc` (naive UTC).

        Built by taking the full state of that IST day and reverting the changes made
        at/after the instant (a driver changes at most once per day), so IDs and
        attributes are identical to the full-day computation.
        """
        day = (instant_utc + IST_OFFSET).date()
        if day < self.cfg.world_start:
            return self._base
        prev = self.fleet_as_of(day - timedelta(days=1))
        full = self.fleet_as_of(day)
        d = full.drivers
        is_new = d["onboarded_at_utc"] >= instant_utc
        changed_later = (d["updated_at_utc"] >= instant_utc) & ~is_new
        kept = d[~is_new & ~changed_later]
        reverted = prev.drivers[prev.drivers["driver_id"].isin(d.loc[changed_later, "driver_id"])]
        drivers = pd.concat([kept, reverted]).sort_values("driver_id", kind="stable").reset_index(drop=True)
        vehicles = full.vehicles[full.vehicles["created_at_utc"] < instant_utc].reset_index(drop=True)
        return FleetState(drivers, vehicles, dict(full.next_driver_seq), dict(full.next_vehicle_seq))
