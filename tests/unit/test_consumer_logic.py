"""Landing logic with an in-memory Kafka: quarantine routing, commit-after-write, at-least-once."""
from datetime import date

import duckdb

from kafka_fakes import FakeConsumer, FakeLog, emit_day, land
from porterflow.ingestion.consumer import parse_message

DAY = date(2026, 9, 1)


def test_parse_message_routes_bad_messages():
    row, err = parse_message("trip_events", b'{"event_id": 1')
    assert row is None and "invalid JSON" in err
    row, err = parse_message("driver_sessions", b'{"event_id":"e","schema_version":1,"event_type":"driver_login",'
                                                b'"event_ts":"2026-09-01T00:00:00Z","driver_id":"D","city_code":"PUN"}')
    assert err is None and row["city_code"] == "PUN"


def test_landing_commits_offsets_and_quarantines(settings):
    log, consumer = FakeLog(settings), FakeConsumer()
    emit_day(log, settings, DAY)
    total = sum(len(v) for v in log.logs.values())
    manifest = land(log, consumer, settings, DAY)
    landed = manifest["valid"]["trip_events"] + manifest["valid"]["driver_sessions"] + manifest["quarantined"]
    assert landed == total
    assert consumer.committed == {k: len(v) for k, v in log.logs.items() if v}  # next offset to read
    assert manifest["quarantined"] > 0
    q = duckdb.sql(f"select count(*) from read_parquet('{settings.quarantine_dir}/*/*/*.parquet')").fetchone()[0]
    assert q == manifest["quarantined"]
    # nothing new -> nothing landed
    assert land(log, consumer, settings, DAY)["valid"]["trip_events"] == 0


def test_crash_before_commit_relands_into_the_same_files(settings):
    """Files written, offset commit lost (crash): the next run re-reads the same offsets and
    overwrites the same deterministic file names, so the raw zone does not grow."""
    log, consumer = FakeLog(settings), FakeConsumer()
    emit_day(log, settings, DAY)
    land(log, consumer, settings, DAY)
    raw = f"read_parquet('{settings.raw_table_dir('trip_events')}/*/*/*.parquet', hive_partitioning = true)"
    files_before = sorted(settings.raw_table_dir("trip_events").rglob("*.parquet"))
    rows_before = duckdb.sql(f"select count(*) from {raw}").fetchone()[0]

    consumer.committed.clear()
    again = land(log, consumer, settings, DAY)

    assert again["valid"]["trip_events"] == rows_before          # everything was redelivered...
    assert sorted(settings.raw_table_dir("trip_events").rglob("*.parquet")) == files_before
    assert duckdb.sql(f"select count(*) from {raw}").fetchone()[0] == rows_before   # ...but not duplicated
