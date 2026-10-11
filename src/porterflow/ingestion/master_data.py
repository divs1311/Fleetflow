"""Daily batch extract of driver and vehicle master data from the REST API.

Incremental by design: each run asks only for records changed inside its data
interval [00:00 IST of run_date, 00:00 IST of run_date + 1). The very first run
(no earlier extract in the lake) does a full load. Because the window is fixed
by the run date, not by "now", re-running a day returns the same rows and
overwrites the same partition: the task is idempotent and safe to backfill.
"""
from __future__ import annotations

import json
import logging
import random
import time
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import requests

from ..config import Settings
from ..schemas import DRIVER_MASTER_SCHEMA, VEHICLE_MASTER_SCHEMA, parse_utc
from ..simulator.world import ist_midnight_utc
from .raw_writer import write_table_atomic

log = logging.getLogger(__name__)

RETRYABLE = {429, 500, 502, 503, 504}
EPOCH = datetime(1970, 1, 1)
RESOURCES = {"drivers": DRIVER_MASTER_SCHEMA, "vehicles": VEHICLE_MASTER_SCHEMA}


def _iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S") + "Z"


def get_with_retries(session: requests.Session, url: str, params: dict, max_attempts: int = 6,
                     base_delay: float = 0.5) -> dict:
    """GET with exponential backoff + jitter; honours Retry-After on 429/503."""
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, params=params, timeout=(5, 60))
        except (requests.ConnectionError, requests.Timeout) as exc:
            if attempt == max_attempts:
                raise
            delay = base_delay * 2 ** (attempt - 1) + random.uniform(0, base_delay)
            log.warning("network error %s (attempt %d), retrying in %.1fs", exc.__class__.__name__, attempt, delay)
            time.sleep(delay)
            continue
        if resp.status_code in RETRYABLE and attempt < max_attempts:
            retry_after = resp.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else base_delay * 2 ** (attempt - 1) + random.uniform(0, base_delay)
            log.warning("HTTP %d from %s (attempt %d), retrying in %.1fs", resp.status_code, url, attempt, delay)
            time.sleep(delay)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError("unreachable")


def fetch_all(session: requests.Session, url: str, since: datetime, until: datetime, page_size: int = 1000) -> list[dict]:
    records, page, expected = [], 1, None
    while page:
        body = get_with_retries(session, url, {"updated_since": _iso(since), "updated_until": _iso(until),
                                                "page": page, "page_size": page_size})
        if expected is None:
            expected = body["total_records"]
        records.extend(body["data"])
        page = body["next_page"]
    if len(records) != expected:
        raise RuntimeError(f"pagination drift on {url}: expected {expected} records, got {len(records)}")
    return records


def _earlier_extract_exists(settings: Settings, resource: str, run_date: date) -> bool:
    base = settings.master_dir / resource
    if not base.exists():
        return False
    return any(p.name.startswith("extract_date=") and p.name.split("=", 1)[1] < run_date.isoformat()
               and (p / "part-0.parquet").exists() for p in base.iterdir())


def _to_table(records: list[dict], schema: pa.Schema, extracted_at: datetime, since: datetime,
              until: datetime) -> pa.Table:
    rows = []
    for r in records:
        row = {}
        for field in schema:
            if field.name == "_extracted_at_utc":
                row[field.name] = extracted_at
            elif field.name == "_window_start_utc":
                row[field.name] = since
            elif field.name == "_window_end_utc":
                row[field.name] = until
            elif field.name.endswith("_utc"):
                row[field.name] = parse_utc(r[field.name], field.name) if r.get(field.name) else None
            else:
                row[field.name] = r.get(field.name)
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=schema)


def extract_master_data(settings: Settings, run_date: date) -> dict:
    until = ist_midnight_utc(run_date + timedelta(days=1))
    extracted_at = datetime.now(timezone.utc).replace(tzinfo=None)
    session = requests.Session()
    session.headers.update({"X-API-Key": settings.master_api_key, "Accept": "application/json"})
    summary = {"run_date": run_date.isoformat(), "resources": {}}
    for resource, schema in RESOURCES.items():
        full_load = not _earlier_extract_exists(settings, resource, run_date)
        since = EPOCH if full_load else ist_midnight_utc(run_date)
        records = fetch_all(session, f"{settings.master_api_url}/v1/{resource}", since, until)
        table = _to_table(records, schema, extracted_at, since, until)
        id_col = "driver_id" if resource == "drivers" else "vehicle_id"
        ids = table.column(id_col).to_pylist()
        if len(ids) != len(set(ids)):
            raise RuntimeError(f"{resource}: duplicate {id_col} in a single extract")
        path = settings.master_dir / resource / f"extract_date={run_date.isoformat()}" / "part-0.parquet"
        write_table_atomic(table, path)
        summary["resources"][resource] = {"rows": table.num_rows, "full_load": full_load,
                                          "window_start_utc": _iso(since), "window_end_utc": _iso(until)}
        log.info("extracted %s: %s", resource, summary["resources"][resource])
    out = settings.run_dir(run_date) / "master_extract.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2))
    return summary
