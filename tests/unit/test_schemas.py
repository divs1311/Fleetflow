import json
from datetime import date, datetime

import pytest

from porterflow import schemas
from porterflow.schemas import ContractViolation, decode_json, validate_session_event, validate_trip_event

GOOD = {
    "event_id": "abc123", "schema_version": 2, "event_type": "trip_delivered",
    "event_ts": "2026-09-01T10:00:00.123Z", "produced_at": "2026-09-01T10:00:02Z",
    "trip_id": "TRP1", "city_code": "BLR", "vehicle_type": "tata_ace", "driver_id": "DRV1", "vehicle_id": "VEH1",
    "lat": 12.9, "lng": 77.6, "fare_amount": 410.0, "distance_km": 9.2, "payment_mode": "upi", "app_version": "6.5.0",
}


def test_valid_event_is_typed():
    row = validate_trip_event(dict(GOOD))
    assert row["event_ts_utc"] == datetime(2026, 9, 1, 10, 0, 0, 123000)
    assert row["event_ts_utc"].tzinfo is None  # stored as naive UTC
    assert row["fare_amount"] == 410.0 and row["drop_lat"] is None


def test_v1_event_without_new_field_is_accepted():
    rec = dict(GOOD, schema_version=1)
    rec.pop("app_version")
    assert validate_trip_event(rec)["app_version"] is None


def test_non_utc_offset_is_normalised():
    row = validate_trip_event(dict(GOOD, event_ts="2026-09-01T15:30:00+05:30"))
    assert row["event_ts_utc"] == datetime(2026, 9, 1, 10, 0)


@pytest.mark.parametrize("change, message", [
    ({"trip_id": None}, "trip_id"),
    ({"city_code": "XXX"}, "city_code"),
    ({"event_ts": "2026-13-45T25:61:00Z"}, "unparseable"),
    ({"event_ts": "2026-09-01T10:00:00"}, "no timezone"),
    ({"event_type": "teleported"}, "event_type"),
    ({"schema_version": 7}, "schema_version"),
    ({"lat": 91.0}, "out of range"),
    ({"lng": None}, "only one coordinate"),
    ({"fare_amount": "410"}, "expected a number"),
    ({"vehicle_type": "rocket"}, "vehicle_type"),
])
def test_contract_violations_are_rejected(change, message):
    with pytest.raises(ContractViolation, match=message):
        validate_trip_event({**GOOD, **change})


def test_assignment_needs_driver_and_cancellation_needs_actor():
    with pytest.raises(ContractViolation):
        validate_trip_event(dict(GOOD, event_type="driver_assigned", driver_id=None))
    with pytest.raises(ContractViolation):
        validate_trip_event(dict(GOOD, event_type="trip_cancelled", cancelled_by="alien"))


@pytest.mark.parametrize("raw", [None, b"", b"{not json", b"[1, 2]", b'{"a": 1'])
def test_decode_rejects_garbage(raw):
    with pytest.raises(ContractViolation):
        decode_json(raw)


def test_session_event():
    rec = {"event_id": "e", "schema_version": 1, "event_type": "driver_login", "event_ts": "2026-09-01T01:00:00Z",
           "driver_id": "DRV1", "city_code": "MUM", "lat": None, "lng": None}
    assert validate_session_event(rec)["driver_id"] == "DRV1"
    with pytest.raises(ContractViolation):
        validate_session_event(dict(rec, driver_id=None))


def test_ist_business_date_crosses_midnight_correctly():
    assert schemas.ist_date_of(datetime(2026, 9, 1, 18, 29)) == date(2026, 9, 1)   # 23:59 IST
    assert schemas.ist_date_of(datetime(2026, 9, 1, 18, 30)) == date(2026, 9, 2)   # 00:00 IST


def test_payload_round_trip_through_json():
    assert validate_trip_event(decode_json(json.dumps(GOOD).encode()))["trip_id"] == "TRP1"
