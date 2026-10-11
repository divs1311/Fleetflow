"""One simulated business day (IST) of trips and driver shifts.

A dispatch loop matches each booking to the driver with the lowest ETA
(waiting until the driver is free + travelling empty to the pickup), so
utilisation, dead-km and 'no driver found' cancellations emerge from supply and
demand rather than being drawn at random. The output is the *clean truth*;
`mess.py` then corrupts it the way real mobile event streams are corrupted.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd

from .world import CITIES, VEHICLE_SPECS, SHIFT_TYPES, World, ist_midnight_utc, random_point

HOURLY_DEMAND = np.array([0.25, 0.15, 0.10, 0.08, 0.10, 0.25, 0.60, 1.00, 1.50, 1.90, 2.00, 1.90,
                          1.70, 1.50, 1.40, 1.40, 1.60, 1.80, 1.90, 1.70, 1.30, 0.90, 0.60, 0.40])
HOURLY_DEMAND = HOURLY_DEMAND / HOURLY_DEMAND.sum()
WEEKDAY_FACTOR = (1.00, 1.02, 1.03, 1.04, 1.06, 0.92, 0.78)  # Mon..Sun
DAY_OFF_PROB = 0.15
ROAD_FACTOR = 1.35            # road km / straight-line km
MAX_SEARCH_S = 480            # dispatch gives up after 8 minutes
SHIFT_START = {"morning": (6.8, 0.6, 5.5, 8.5), "day": (9.5, 0.7, 8.0, 11.5),
               "evening": (14.8, 0.8, 13.0, 16.5), "night": (0.4, 0.3, 0.0, 1.2)}

PRE_ASSIGN_CUSTOMER_REASONS = (("booked_by_mistake", 0.35), ("long_wait_for_assignment", 0.65))
POST_ASSIGN_CUSTOMER_REASONS = (("driver_too_far", 0.35), ("changed_plans", 0.30),
                                ("found_alternative", 0.20), ("driver_asked_extra_payment", 0.15))
DRIVER_REASONS = (("pickup_location_unreachable", 0.35), ("driver_personal_reason", 0.25),
                  ("vehicle_breakdown", 0.20), ("customer_unreachable", 0.20))


def haversine_km(lat1, lng1, lat2, lng2):
    lat1, lng1, lat2, lng2 = map(np.radians, (lat1, lng1, lat2, lng2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lng2 - lng1) / 2) ** 2
    return 6371.0 * 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def speed_factor(hour: float, rain: bool) -> float:
    h = int(hour) % 24
    f = 0.75 if (8 <= h <= 11 or 17 <= h <= 21) else (1.35 if (h >= 22 or h <= 5) else 1.0)
    return f * (0.7 if rain else 1.0)


def surge(hour: float, rain: bool) -> float:
    h = int(hour) % 24
    return (1.35 if rain else 1.0) * (1.15 if (8 <= h <= 11 or 17 <= h <= 21) else 1.0)


def fare_for(vtype: str, road_km: float, hour: float, rain: bool) -> float:
    spec = VEHICLE_SPECS[vtype]
    return float(round((spec.base_fare + spec.per_km * max(0.0, road_km - 2.0)) * surge(hour, rain)))


def _pick(rng, options):
    names, probs = zip(*options)
    return names[rng.choice(len(names), p=np.array(probs) / sum(probs))]


@dataclass
class DaySimulation:
    day: date
    trip_events: pd.DataFrame
    session_events: pd.DataFrame
    trips: pd.DataFrame


def simulate_day(world: World, day: date) -> DaySimulation:
    cfg = world.cfg
    rng = np.random.default_rng([cfg.seed, day.toordinal(), 1])
    fleet = world.fleet_as_of(day)
    drivers = fleet.drivers[fleet.drivers["status"] == "active"].reset_index(drop=True)
    rain_cities = {city for d_, city in cfg.rain_events if d_ == day}
    # Fewer trips in the same city area means drivers are sparser: widen the dispatch radius so a
    # small simulation (e.g. in tests) behaves like a large one instead of starving for drivers.
    density_factor = min(4.0, max(1.0, math.sqrt(20_000 / cfg.trips_per_day)))
    midnight_utc = ist_midnight_utc(day)

    # ---- driver shifts ------------------------------------------------------
    n_drv = len(drivers)
    on_shift = rng.random(n_drv) >= DAY_OFF_PROB
    start_h = np.empty(n_drv)
    dur_h = np.empty(n_drv)
    for st in SHIFT_TYPES:
        m = (drivers["shift_type"] == st).to_numpy()
        mu, sd, lo, hi = SHIFT_START[st]
        start_h[m] = np.clip(rng.normal(mu, sd, m.sum()), lo, hi)
        dur_h[m] = np.clip(rng.normal(7.5 if st == "night" else 9.5, 0.9, m.sum()), 6.0, 12.0)
    login_s = start_h * 3600.0
    logout_s = login_s + dur_h * 3600.0
    free_at = login_s.copy()
    cur_lat = drivers["home_lat"].to_numpy(copy=True)
    cur_lng = drivers["home_lng"].to_numpy(copy=True)
    last_busy_end = np.zeros(n_drv)
    driver_ids = drivers["driver_id"].to_numpy()
    vehicle_ids = drivers["vehicle_id"].to_numpy()

    # ---- demand ----------------------------------------------------------------
    growth = 1.001 ** max(0, (day - cfg.world_start).days)
    total = cfg.trips_per_day * WEEKDAY_FACTOR[day.weekday()] * growth * rng.lognormal(0, 0.04)
    trip_rows = []
    for city_code, city in CITIES.items():
        rain = city_code in rain_cities
        n_city = int(rng.poisson(total * city.demand_weight * (1.35 if rain else 1.0)))
        shares = np.array([s.demand_share for s in VEHICLE_SPECS.values()])
        per_type = rng.multinomial(n_city, shares / shares.sum())
        customer_pool = max(50, int(cfg.trips_per_day * city.demand_weight * 8))
        city_seq = 0
        for vtype, n_trips in zip(VEHICLE_SPECS, per_type):
            if n_trips == 0:
                continue
            spec = VEHICLE_SPECS[vtype]
            hours = rng.choice(24, size=n_trips, p=HOURLY_DEMAND)
            booked_s = np.sort(hours * 3600.0 + rng.random(n_trips) * 3600.0)
            p_lat, p_lng = random_point(rng, city.lat, city.lng, city.radius_km, n_trips)
            straight = rng.lognormal(math.log(spec.median_trip_km / ROAD_FACTOR), 0.55, n_trips).clip(0.5, 60)
            bearing = rng.random(n_trips) * 2 * math.pi
            d_lat = p_lat + straight * np.cos(bearing) / 111.0
            d_lng = p_lng + straight * np.sin(bearing) / (111.0 * math.cos(math.radians(city.lat)))
            patience = rng.exponential(900, n_trips) + 120
            mistake = rng.random(n_trips) < 0.015
            u_post = rng.random(n_trips)
            cust_idx = (customer_pool * rng.random(n_trips) ** 2).astype(int)
            pool = np.flatnonzero(((drivers["city_code"] == city_code) & (drivers["vehicle_type"] == vtype)).to_numpy()
                                  & on_shift)
            for i in range(n_trips):
                city_seq += 1
                t = booked_s[i]
                road_km = straight[i] * ROAD_FACTOR
                hour = t / 3600.0
                rec = {
                    "trip_id": f"TRP{day:%y%m%d}{city_code}{vtype[:2].upper()}{city_seq:06d}",
                    "city_code": city_code, "vehicle_type": vtype,
                    "customer_id": f"CUS{city_code}{cust_idx[i]:07d}",
                    "booked_s": t, "pickup_lat": p_lat[i], "pickup_lng": p_lng[i],
                    "drop_lat": d_lat[i], "drop_lng": d_lng[i], "planned_km": road_km,
                    "estimated_fare": fare_for(vtype, road_km, hour, rain),
                    "status": None, "driver_idx": -1, "assign_s": np.nan, "drv_lat": np.nan, "drv_lng": np.nan,
                    "approach_km": np.nan, "arrive_s": np.nan, "picked_s": np.nan, "delivered_s": np.nan,
                    "cancel_s": np.nan, "cancelled_by": None, "cancellation_reason": None,
                    "actual_km": np.nan, "fare_amount": np.nan, "payment_mode": None,
                    "cancel_lat": np.nan, "cancel_lng": np.nan,
                }
                trip_rows.append(rec)
                if mistake[i]:
                    rec.update(status="cancelled", cancel_s=t + rng.uniform(10, 120), cancelled_by="customer",
                               cancellation_reason="booked_by_mistake")
                    continue
                # ---- dispatch: lowest ETA among drivers who will be free soon
                best = -1
                if pool.size:
                    avail = np.maximum(t, free_at[pool])
                    ok = (login_s[pool] <= t + 60) & (logout_s[pool] >= t + 1200) & (avail <= t + MAX_SEARCH_S)
                    if ok.any():
                        cand = pool[ok]
                        dist = haversine_km(cur_lat[cand], cur_lng[cand], p_lat[i], p_lng[i]) * ROAD_FACTOR
                        near = dist <= spec.max_approach_km * density_factor
                        if near.any():
                            cand, dist, av = cand[near], dist[near], avail[ok][near]
                            spd = spec.speed_kmph * speed_factor(hour, rain)
                            eta = (av - t) + dist / spd * 3600.0
                            j = int(np.argmin(eta))
                            best, best_dist, best_avail = int(cand[j]), float(dist[j]), float(av[j])
                if best < 0:
                    if patience[i] < MAX_SEARCH_S:
                        rec.update(status="cancelled", cancel_s=t + patience[i], cancelled_by="customer",
                                   cancellation_reason="long_wait_for_assignment")
                    else:
                        rec.update(status="cancelled", cancel_s=t + MAX_SEARCH_S, cancelled_by="system",
                                   cancellation_reason="no_driver_found")
                    continue
                assign_s = best_avail + rng.uniform(5, 40)
                if assign_s > t + patience[i]:
                    rec.update(status="cancelled", cancel_s=t + patience[i], cancelled_by="customer",
                               cancellation_reason="long_wait_for_assignment")
                    continue
                spd = spec.speed_kmph * speed_factor(assign_s / 3600.0, rain)
                approach_s = best_dist / spd * 3600.0
                rec.update(driver_idx=best, assign_s=assign_s, drv_lat=cur_lat[best], drv_lng=cur_lng[best],
                           approach_km=best_dist)
                eta_min = approach_s / 60.0
                p_driver = 0.025
                p_cust = min(0.25, 0.03 + 0.004 * eta_min)
                if u_post[i] < p_driver + p_cust:
                    by_driver = u_post[i] < p_driver
                    cancel_s = assign_s + (rng.uniform(60, max(120, 0.7 * approach_s)) if by_driver
                                           else rng.uniform(30, max(60, approach_s)))
                    frac = min(1.0, (cancel_s - assign_s) / max(approach_s, 1.0))
                    lat = cur_lat[best] + frac * (p_lat[i] - cur_lat[best])
                    lng = cur_lng[best] + frac * (p_lng[i] - cur_lng[best])
                    reason = _pick(rng, DRIVER_REASONS if by_driver else POST_ASSIGN_CUSTOMER_REASONS)
                    if not by_driver and eta_min > 15 and rng.random() < 0.5:
                        reason = "driver_too_far"
                    rec.update(status="cancelled", cancel_s=cancel_s, cancelled_by="driver" if by_driver else "customer",
                               cancellation_reason=reason, cancel_lat=lat, cancel_lng=lng)
                    free_at[best], cur_lat[best], cur_lng[best] = cancel_s, lat, lng
                    last_busy_end[best] = max(last_busy_end[best], cancel_s)
                    continue
                lo, hi = spec.load_minutes
                arrive_s = assign_s + approach_s
                picked_s = arrive_s + rng.uniform(lo, hi) * 60.0
                actual_km = road_km * rng.normal(1.0, 0.03)
                travel_s = actual_km / (spec.speed_kmph * speed_factor(picked_s / 3600.0, rain)) * 3600.0
                delivered_s = picked_s + travel_s + rng.uniform(lo, hi) * 30.0
                rec.update(status="completed", arrive_s=arrive_s, picked_s=picked_s, delivered_s=delivered_s,
                           actual_km=actual_km, fare_amount=fare_for(vtype, actual_km, picked_s / 3600.0, rain),
                           payment_mode=rng.choice(("upi", "cash", "wallet"), p=(0.55, 0.35, 0.10)))
                free_at[best], cur_lat[best], cur_lng[best] = delivered_s, d_lat[i], d_lng[i]
                last_busy_end[best] = max(last_busy_end[best], delivered_s)

    trips = pd.DataFrame(trip_rows)
    trips["driver_id"] = np.where(trips["driver_idx"] >= 0, driver_ids[trips["driver_idx"].clip(lower=0)], None)
    trips["vehicle_id"] = np.where(trips["driver_idx"] >= 0, vehicle_ids[trips["driver_idx"].clip(lower=0)], None)

    trip_events = _trip_events(trips, midnight_utc, rng)
    session_events = _session_events(drivers, on_shift, login_s, logout_s, last_busy_end, cur_lat, cur_lng,
                                     midnight_utc, rng)
    return DaySimulation(day, trip_events, session_events, trips)


def _to_utc(midnight_utc, seconds: pd.Series) -> pd.Series:
    return pd.to_datetime(midnight_utc) + pd.to_timedelta(seconds.round(3), unit="s")


def _gps_noise(rng, n):
    return rng.normal(0, 0.0002, n)


def _trip_events(trips: pd.DataFrame, midnight_utc, rng) -> pd.DataFrame:
    base_cols = ["trip_id", "city_code", "customer_id", "vehicle_type"]
    frames = []

    b = trips[base_cols].copy()
    b["event_type"] = "trip_booked"
    b["event_ts_utc"] = _to_utc(midnight_utc, trips["booked_s"])
    b["lat"], b["lng"] = trips["pickup_lat"], trips["pickup_lng"]
    for c in ("pickup_lat", "pickup_lng", "drop_lat", "drop_lng", "estimated_fare"):
        b[c] = trips[c]
    frames.append(b)

    a_mask = trips["driver_idx"] >= 0
    a = trips.loc[a_mask, base_cols + ["driver_id", "vehicle_id"]].copy()
    a["event_type"] = "driver_assigned"
    a["event_ts_utc"] = _to_utc(midnight_utc, trips.loc[a_mask, "assign_s"])
    a["lat"], a["lng"] = trips.loc[a_mask, "drv_lat"], trips.loc[a_mask, "drv_lng"]
    a["approach_km_truth"] = trips.loc[a_mask, "approach_km"]
    frames.append(a)

    c_mask = trips["status"] == "completed"
    comp = trips[c_mask]
    p = comp[base_cols + ["driver_id", "vehicle_id"]].copy()
    p["event_type"] = "trip_picked_up"
    p["event_ts_utc"] = _to_utc(midnight_utc, comp["picked_s"])
    p["lat"] = comp["pickup_lat"] + _gps_noise(rng, len(comp))
    p["lng"] = comp["pickup_lng"] + _gps_noise(rng, len(comp))
    frames.append(p)

    d = comp[base_cols + ["driver_id", "vehicle_id"]].copy()
    d["event_type"] = "trip_delivered"
    d["event_ts_utc"] = _to_utc(midnight_utc, comp["delivered_s"])
    d["lat"] = comp["drop_lat"] + _gps_noise(rng, len(comp))
    d["lng"] = comp["drop_lng"] + _gps_noise(rng, len(comp))
    d["fare_amount"] = comp["fare_amount"]
    d["distance_km"] = comp["actual_km"].round(3)
    d["payment_mode"] = comp["payment_mode"]
    frames.append(d)

    x_mask = trips["status"] == "cancelled"
    canc = trips[x_mask]
    x = canc[base_cols + ["driver_id", "vehicle_id"]].copy()
    x["event_type"] = "trip_cancelled"
    x["event_ts_utc"] = _to_utc(midnight_utc, canc["cancel_s"])
    pre = canc["driver_idx"] < 0
    x["lat"] = np.where(pre, canc["pickup_lat"], canc["cancel_lat"])
    x["lng"] = np.where(pre, canc["pickup_lng"], canc["cancel_lng"])
    x["cancelled_by"] = canc["cancelled_by"]
    x["cancellation_reason"] = canc["cancellation_reason"]
    frames.append(x)

    ev = pd.concat(frames, ignore_index=True)
    order = {"trip_booked": 0, "driver_assigned": 1, "trip_picked_up": 2, "trip_delivered": 3, "trip_cancelled": 4}
    ev["_seq"] = ev["event_type"].map(order)
    ev = ev.sort_values(["event_ts_utc", "trip_id", "_seq"], kind="stable").drop(columns="_seq").reset_index(drop=True)
    return ev


def _session_events(drivers, on_shift, login_s, logout_s, last_busy_end, cur_lat, cur_lng, midnight_utc, rng):
    idx = np.flatnonzero(on_shift)
    d = drivers.iloc[idx]
    end_s = np.maximum(logout_s[idx], last_busy_end[idx] + rng.uniform(30, 300, idx.size))
    login = pd.DataFrame({
        "event_type": "driver_login", "driver_id": d["driver_id"].to_numpy(),
        "vehicle_id": d["vehicle_id"].to_numpy(), "city_code": d["city_code"].to_numpy(),
        "event_ts_utc": _to_utc(midnight_utc, pd.Series(login_s[idx])),
        "lat": d["home_lat"].to_numpy(), "lng": d["home_lng"].to_numpy(),
    })
    logout = pd.DataFrame({
        "event_type": "driver_logout", "driver_id": d["driver_id"].to_numpy(),
        "vehicle_id": d["vehicle_id"].to_numpy(), "city_code": d["city_code"].to_numpy(),
        "event_ts_utc": _to_utc(midnight_utc, pd.Series(end_s)),
        "lat": cur_lat[idx], "lng": cur_lng[idx],
    })
    ev = pd.concat([login, logout], ignore_index=True)
    return ev.sort_values(["event_ts_utc", "driver_id"], kind="stable").reset_index(drop=True)


def emission_window(day: date, max_late_days: int = 3) -> list[date]:
    return [day - timedelta(days=k) for k in range(max_late_days, -1, -1)]
