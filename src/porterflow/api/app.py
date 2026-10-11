"""Mock driver/vehicle master-data REST API (stands in for an internal ops service).

It behaves like a real, slightly unreliable internal API:
  * API-key auth (X-API-Key header)
  * incremental filters: updated_since <= updated_at < updated_until
  * page-number pagination with a stable sort (updated_at, id)
  * random 503s and 429s (with Retry-After) at API_FAILURE_RATE
Responses are generated from the deterministic simulator, so a backfill that
asks for an old window gets exactly the same answer every time.

Run locally:  uvicorn porterflow.api.app:app --port 8000
"""
from __future__ import annotations

import os
import random
from datetime import datetime

import pandas as pd
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse

from ..schemas import ContractViolation, parse_utc
from ..simulator import SimConfig, World

API_KEY = os.environ.get("MASTER_API_KEY", "porterflow-dev-key")
FAILURE_RATE = float(os.environ.get("API_FAILURE_RATE", "0.1"))
MAX_PAGE_SIZE = 1000
EPOCH = "1970-01-01T00:00:00Z"

world = World(SimConfig(seed=int(os.environ.get("SIM_SEED", "42")),
                        trips_per_day=int(os.environ.get("SIM_TRIPS_PER_DAY", "20000"))))
app = FastAPI(title="PorterFlow master-data API (mock)", version="1.0.0")

DRIVER_FIELDS = ["driver_id", "full_name", "phone_masked", "city_code", "vehicle_id", "vehicle_type", "shift_type",
                 "status", "rating", "onboarded_at_utc", "updated_at_utc"]
VEHICLE_FIELDS = ["vehicle_id", "registration_no", "vehicle_type", "city_code", "fuel_type", "make_model",
                  "capacity_kg", "created_at_utc", "updated_at_utc"]


def _parse(value: str, name: str) -> datetime:
    try:
        return parse_utc(value, name)
    except ContractViolation as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _maybe_fail() -> JSONResponse | None:
    if FAILURE_RATE > 0 and random.random() < FAILURE_RATE:
        if random.random() < 0.5:
            return JSONResponse(status_code=429, content={"error": "rate limited"}, headers={"Retry-After": "1"})
        return JSONResponse(status_code=503, content={"error": "service temporarily unavailable"})
    return None


def _page(df: pd.DataFrame, id_col: str, fields: list[str], since: datetime, until: datetime,
          page: int, page_size: int) -> dict:
    sel = df[(df["updated_at_utc"] >= since) & (df["updated_at_utc"] < until)]
    sel = sel.sort_values(["updated_at_utc", id_col], kind="stable")
    total = len(sel)
    chunk = sel.iloc[(page - 1) * page_size: page * page_size][fields].copy()
    for col in fields:
        if col.endswith("_utc"):
            chunk[col] = chunk[col].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    records = chunk.astype(object).where(chunk.notna(), None).to_dict("records")
    total_pages = (total + page_size - 1) // page_size
    return {"data": records, "page": page, "page_size": page_size, "total_records": total,
            "total_pages": total_pages, "next_page": page + 1 if page < total_pages else None}


def _check_key(key: str | None) -> None:
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="invalid or missing X-API-Key")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/v1/drivers")
def drivers(updated_until: str, updated_since: str = EPOCH, page: int = Query(1, ge=1),
            page_size: int = Query(500, ge=1, le=MAX_PAGE_SIZE), x_api_key: str | None = Header(None)):
    _check_key(x_api_key)
    if (fail := _maybe_fail()) is not None:
        return fail
    since, until = _parse(updated_since, "updated_since"), _parse(updated_until, "updated_until")
    if since >= until:
        raise HTTPException(status_code=422, detail="updated_since must be before updated_until")
    fleet = world.fleet_before(until)
    return _page(fleet.drivers, "driver_id", DRIVER_FIELDS, since, until, page, page_size)


@app.get("/v1/vehicles")
def vehicles(updated_until: str, updated_since: str = EPOCH, page: int = Query(1, ge=1),
             page_size: int = Query(500, ge=1, le=MAX_PAGE_SIZE), x_api_key: str | None = Header(None)):
    _check_key(x_api_key)
    if (fail := _maybe_fail()) is not None:
        return fail
    since, until = _parse(updated_since, "updated_since"), _parse(updated_until, "updated_until")
    if since >= until:
        raise HTTPException(status_code=422, detail="updated_since must be before updated_until")
    fleet = world.fleet_before(until)
    return _page(fleet.vehicles, "vehicle_id", VEHICLE_FIELDS, since, until, page, page_size)
