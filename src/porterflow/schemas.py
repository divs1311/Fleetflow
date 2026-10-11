"""Data contracts for every dataset that lands in the lake.

The producer (mobile apps, simulated) sends JSON. The raw zone stores typed
Parquet with exactly these columns, so a downstream reader never has to guess
types. Schema evolution is additive only: v2 events added the nullable
`app_version` column, and v1 rows simply carry NULL there.

All timestamps are UTC and stored as timezone-naive values with a `_utc` suffix.
Business dates (partitions, KPIs) are IST calendar dates and are always derived
explicitly with +05:30, never from a machine's local timezone.
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any

import pyarrow as pa

CITY_CODES = ("BLR", "MUM", "DEL", "HYD", "PUN", "CHE")
VEHICLE_TYPES = ("two_wheeler", "three_wheeler", "tata_ace", "pickup_8ft", "truck_14ft")
TRIP_EVENT_TYPES = ("trip_booked", "driver_assigned", "trip_picked_up", "trip_delivered", "trip_cancelled")
SESSION_EVENT_TYPES = ("driver_login", "driver_logout")
CANCELLED_BY = ("customer", "driver", "system")
PAYMENT_MODES = ("upi", "cash", "wallet")

# Kafka metadata added by the consumer to every raw row (lineage + replay).
LINEAGE_FIELDS = [
    pa.field("_kafka_topic", pa.string(), nullable=False),
    pa.field("_kafka_partition", pa.int32(), nullable=False),
    pa.field("_kafka_offset", pa.int64(), nullable=False),
    pa.field("_loaded_at_utc", pa.timestamp("us"), nullable=False),
    pa.field("_batch_id", pa.string(), nullable=False),
]

TRIP_EVENT_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("schema_version", pa.int16(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("event_ts_utc", pa.timestamp("us"), nullable=False),
        pa.field("produced_at_utc", pa.timestamp("us")),
        pa.field("trip_id", pa.string(), nullable=False),
        pa.field("city_code", pa.string(), nullable=False),
        pa.field("customer_id", pa.string()),
        pa.field("vehicle_type", pa.string()),
        pa.field("driver_id", pa.string()),
        pa.field("vehicle_id", pa.string()),
        pa.field("lat", pa.float64()),
        pa.field("lng", pa.float64()),
        pa.field("pickup_lat", pa.float64()),
        pa.field("pickup_lng", pa.float64()),
        pa.field("drop_lat", pa.float64()),
        pa.field("drop_lng", pa.float64()),
        pa.field("estimated_fare", pa.float64()),
        pa.field("fare_amount", pa.float64()),
        pa.field("distance_km", pa.float64()),
        pa.field("payment_mode", pa.string()),
        pa.field("cancelled_by", pa.string()),
        pa.field("cancellation_reason", pa.string()),
        pa.field("app_version", pa.string()),
        *LINEAGE_FIELDS,
    ]
)

DRIVER_SESSION_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("schema_version", pa.int16(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("event_ts_utc", pa.timestamp("us"), nullable=False),
        pa.field("produced_at_utc", pa.timestamp("us")),
        pa.field("driver_id", pa.string(), nullable=False),
        pa.field("vehicle_id", pa.string()),
        pa.field("city_code", pa.string(), nullable=False),
        pa.field("lat", pa.float64()),
        pa.field("lng", pa.float64()),
        *LINEAGE_FIELDS,
    ]
)

QUARANTINE_SCHEMA = pa.schema(
    [
        pa.field("_kafka_topic", pa.string(), nullable=False),
        pa.field("_kafka_partition", pa.int32(), nullable=False),
        pa.field("_kafka_offset", pa.int64(), nullable=False),
        pa.field("message_key", pa.string()),
        pa.field("raw_value", pa.string()),
        pa.field("error", pa.string(), nullable=False),
        pa.field("_loaded_at_utc", pa.timestamp("us"), nullable=False),
        pa.field("_batch_id", pa.string(), nullable=False),
    ]
)

DRIVER_MASTER_SCHEMA = pa.schema(
    [
        pa.field("driver_id", pa.string(), nullable=False),
        pa.field("full_name", pa.string()),
        pa.field("phone_masked", pa.string()),
        pa.field("city_code", pa.string(), nullable=False),
        pa.field("vehicle_id", pa.string()),
        pa.field("vehicle_type", pa.string()),
        pa.field("shift_type", pa.string()),
        pa.field("status", pa.string(), nullable=False),
        pa.field("rating", pa.float64()),
        pa.field("onboarded_at_utc", pa.timestamp("us")),
        pa.field("updated_at_utc", pa.timestamp("us"), nullable=False),
        pa.field("_extracted_at_utc", pa.timestamp("us"), nullable=False),
        pa.field("_window_start_utc", pa.timestamp("us"), nullable=False),
        pa.field("_window_end_utc", pa.timestamp("us"), nullable=False),
    ]
)

VEHICLE_MASTER_SCHEMA = pa.schema(
    [
        pa.field("vehicle_id", pa.string(), nullable=False),
        pa.field("registration_no", pa.string()),
        pa.field("vehicle_type", pa.string(), nullable=False),
        pa.field("city_code", pa.string(), nullable=False),
        pa.field("fuel_type", pa.string()),
        pa.field("make_model", pa.string()),
        pa.field("capacity_kg", pa.int32()),
        pa.field("created_at_utc", pa.timestamp("us")),
        pa.field("updated_at_utc", pa.timestamp("us"), nullable=False),
        pa.field("_extracted_at_utc", pa.timestamp("us"), nullable=False),
        pa.field("_window_start_utc", pa.timestamp("us"), nullable=False),
        pa.field("_window_end_utc", pa.timestamp("us"), nullable=False),
    ]
)

PAYLOAD_FIELDS = {
    "trip_events": [f.name for f in TRIP_EVENT_SCHEMA if not f.name.startswith("_")],
    "driver_sessions": [f.name for f in DRIVER_SESSION_SCHEMA if not f.name.startswith("_")],
}


class ContractViolation(ValueError):
    """A message that cannot be loaded into the raw zone; it goes to quarantine."""


def parse_utc(value: Any, field_name: str) -> datetime:
    """Parse an ISO-8601 UTC timestamp ('...Z' or '+00:00') into a naive UTC datetime."""
    if not isinstance(value, str) or not value:
        raise ContractViolation(f"{field_name}: missing or not a string")
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractViolation(f"{field_name}: unparseable timestamp {value!r}") from exc
    if ts.tzinfo is None:
        raise ContractViolation(f"{field_name}: timestamp has no timezone")
    return ts.astimezone(timezone.utc).replace(tzinfo=None)


def _opt_float(record: dict, key: str) -> float | None:
    value = record.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractViolation(f"{key}: expected a number, got {type(value).__name__}")
    value = float(value)
    return None if math.isnan(value) else value


def _opt_str(record: dict, key: str) -> str | None:
    value = record.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ContractViolation(f"{key}: expected a string, got {type(value).__name__}")
    return value


def _required_str(record: dict, key: str) -> str:
    value = _opt_str(record, key)
    if not value:
        raise ContractViolation(f"{key}: required field missing")
    return value


def _check_coordinates(row: dict) -> None:
    for lat_key, lng_key in (("lat", "lng"), ("pickup_lat", "pickup_lng"), ("drop_lat", "drop_lng")):
        lat, lng = row.get(lat_key), row.get(lng_key)
        if (lat is None) != (lng is None):
            raise ContractViolation(f"{lat_key}/{lng_key}: only one coordinate present")
        if lat is not None and not (-90 <= lat <= 90 and -180 <= lng <= 180):
            raise ContractViolation(f"{lat_key}/{lng_key}: out of range")


def decode_json(raw: bytes | None) -> dict:
    if raw is None:
        raise ContractViolation("empty message value")
    try:
        record = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContractViolation(f"invalid JSON: {exc.msg if hasattr(exc, 'msg') else exc}") from exc
    if not isinstance(record, dict):
        raise ContractViolation("message is not a JSON object")
    return record


def validate_trip_event(record: dict) -> dict:
    """Return a row matching TRIP_EVENT_SCHEMA (minus lineage), or raise ContractViolation."""
    event_type = _required_str(record, "event_type")
    if event_type not in TRIP_EVENT_TYPES:
        raise ContractViolation(f"event_type: unknown value {event_type!r}")
    city = _required_str(record, "city_code")
    if city not in CITY_CODES:
        raise ContractViolation(f"city_code: unknown value {city!r}")
    version = record.get("schema_version")
    if version not in (1, 2):
        raise ContractViolation(f"schema_version: unsupported value {version!r}")
    row = {
        "event_id": _required_str(record, "event_id"),
        "schema_version": version,
        "event_type": event_type,
        "event_ts_utc": parse_utc(record.get("event_ts"), "event_ts"),
        "produced_at_utc": parse_utc(record["produced_at"], "produced_at") if record.get("produced_at") else None,
        "trip_id": _required_str(record, "trip_id"),
        "city_code": city,
        "customer_id": _opt_str(record, "customer_id"),
        "vehicle_type": _opt_str(record, "vehicle_type"),
        "driver_id": _opt_str(record, "driver_id"),
        "vehicle_id": _opt_str(record, "vehicle_id"),
        "lat": _opt_float(record, "lat"),
        "lng": _opt_float(record, "lng"),
        "pickup_lat": _opt_float(record, "pickup_lat"),
        "pickup_lng": _opt_float(record, "pickup_lng"),
        "drop_lat": _opt_float(record, "drop_lat"),
        "drop_lng": _opt_float(record, "drop_lng"),
        "estimated_fare": _opt_float(record, "estimated_fare"),
        "fare_amount": _opt_float(record, "fare_amount"),
        "distance_km": _opt_float(record, "distance_km"),
        "payment_mode": _opt_str(record, "payment_mode"),
        "cancelled_by": _opt_str(record, "cancelled_by"),
        "cancellation_reason": _opt_str(record, "cancellation_reason"),
        "app_version": _opt_str(record, "app_version"),
    }
    if row["vehicle_type"] is not None and row["vehicle_type"] not in VEHICLE_TYPES:
        raise ContractViolation(f"vehicle_type: unknown value {row['vehicle_type']!r}")
    if event_type == "driver_assigned" and not row["driver_id"]:
        raise ContractViolation("driver_assigned without driver_id")
    if event_type == "trip_cancelled" and row["cancelled_by"] not in CANCELLED_BY:
        raise ContractViolation(f"cancelled_by: invalid value {row['cancelled_by']!r}")
    _check_coordinates(row)
    return row


def validate_session_event(record: dict) -> dict:
    event_type = _required_str(record, "event_type")
    if event_type not in SESSION_EVENT_TYPES:
        raise ContractViolation(f"event_type: unknown value {event_type!r}")
    city = _required_str(record, "city_code")
    if city not in CITY_CODES:
        raise ContractViolation(f"city_code: unknown value {city!r}")
    version = record.get("schema_version")
    if version not in (1, 2):
        raise ContractViolation(f"schema_version: unsupported value {version!r}")
    row = {
        "event_id": _required_str(record, "event_id"),
        "schema_version": version,
        "event_type": event_type,
        "event_ts_utc": parse_utc(record.get("event_ts"), "event_ts"),
        "produced_at_utc": parse_utc(record["produced_at"], "produced_at") if record.get("produced_at") else None,
        "driver_id": _required_str(record, "driver_id"),
        "vehicle_id": _opt_str(record, "vehicle_id"),
        "city_code": city,
        "lat": _opt_float(record, "lat"),
        "lng": _opt_float(record, "lng"),
    }
    _check_coordinates(row)
    return row


def ist_date_of(ts_utc: datetime):
    """Business (IST) calendar date of a naive UTC timestamp."""
    return (ts_utc + timedelta(minutes=330)).date()
