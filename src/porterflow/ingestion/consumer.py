"""Land Kafka topics into the raw zone (micro-batch consumer, at-least-once).

Delivery semantics, step by step:
  1. auto-commit is OFF; the consumer reads up to `flush_every` messages,
  2. validates each message against the data contract (bad ones -> quarantine),
  3. writes Parquet files (atomic rename, deterministic names),
  4. only then commits offsets synchronously.
A crash between 3 and 4 means the next run re-reads those messages: data is never
lost, but may be landed twice. Downstream deduplication on event_id turns this
at-least-once landing into effectively-once results.

The run stops once every assigned partition has reached its end (PARTITION_EOF),
which makes the streaming topic consumable from a daily batch orchestrator.
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import date, datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from .. import schemas
from ..config import STREAMS, Settings
from .kafka_admin import ensure_topics
from .raw_writer import write_partitioned

log = logging.getLogger(__name__)

VALIDATORS = {"trip_events": schemas.validate_trip_event, "driver_sessions": schemas.validate_session_event}
RAW_SCHEMAS = {"trip_events": schemas.TRIP_EVENT_SCHEMA, "driver_sessions": schemas.DRIVER_SESSION_SCHEMA}


def parse_message(stream: str, value: bytes | None) -> tuple[dict | None, str | None]:
    """Return (row, None) for a valid message or (None, error) for one to quarantine."""
    try:
        record = schemas.decode_json(value)
        return VALIDATORS[stream](record), None
    except schemas.ContractViolation as exc:
        return None, str(exc)


class RawLander:
    def __init__(self, settings: Settings, run_date: date):
        self.settings = settings
        self.run_date = run_date
        self.stream_of = {settings.topic(stream): stream for stream in STREAMS}
        self.rows: dict[str, list[dict]] = {stream: [] for stream in STREAMS}
        self.quarantine: list[dict] = []
        self.next_offsets: dict[tuple[str, int], int] = {}
        self.totals = {"valid": {stream: 0 for stream in STREAMS}, "quarantined": 0, "files": 0}
        self.batches: list[str] = []

    def add(self, msg) -> None:
        topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
        stream = self.stream_of[topic]
        row, error = parse_message(stream, msg.value())
        lineage = {"_kafka_topic": topic, "_kafka_partition": partition, "_kafka_offset": offset}
        if row is None:
            key = msg.key()
            raw = msg.value()
            self.quarantine.append({
                **lineage,
                "message_key": key.decode("utf-8", "replace") if key else None,
                "raw_value": raw.decode("utf-8", "replace") if raw is not None else None,
                "error": error,
            })
        else:
            row.update(lineage)
            row["__event_date"] = schemas.ist_date_of(row["event_ts_utc"]).isoformat()
            row["__city"] = row["city_code"]
            self.rows[stream].append(row)
        self.next_offsets[(topic, partition)] = offset + 1

    @property
    def buffered(self) -> int:
        return sum(len(v) for v in self.rows.values()) + len(self.quarantine)

    def flush(self, consumer: Consumer) -> None:
        if not self.next_offsets:
            return
        loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
        batch_id = f"{self.run_date.isoformat()}-{uuid.uuid4().hex[:10]}"
        for stream, rows in self.rows.items():
            if not rows:
                continue
            for r in rows:
                r["_loaded_at_utc"], r["_batch_id"] = loaded_at, batch_id
            written = write_partitioned(rows, RAW_SCHEMAS[stream], self.settings.raw_table_dir(stream),
                                        ("event_date", "city"))
            self.totals["valid"][stream] += len(rows)
            self.totals["files"] += len(written)
        if self.quarantine:
            for r in self.quarantine:
                r["_loaded_at_utc"], r["_batch_id"] = loaded_at, batch_id
                r["__topic"], r["__load_date"] = self.stream_of[r["_kafka_topic"]], self.run_date.isoformat()
            written = write_partitioned(self.quarantine, schemas.QUARANTINE_SCHEMA, self.settings.quarantine_dir,
                                        ("topic", "load_date"))
            self.totals["quarantined"] += len(self.quarantine)
            self.totals["files"] += len(written)
        # Commit only after the files are durably renamed into place (at-least-once).
        offsets = [TopicPartition(t, p, o) for (t, p), o in sorted(self.next_offsets.items())]
        consumer.commit(offsets=offsets, asynchronous=False)
        self.batches.append(batch_id)
        log.info("flushed batch %s: %s", batch_id, {k: len(r) for k, r in self.rows.items()})
        self.rows = {stream: [] for stream in STREAMS}
        self.quarantine = []
        self.next_offsets = {}


def consume_to_raw(settings: Settings, run_date: date, flush_every: int = 200_000,
                   idle_timeout_s: float = 15.0, max_runtime_s: float = 1800.0) -> dict:
    ensure_topics(settings.kafka_bootstrap, settings.topic_partitions)
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap,
        "group.id": settings.consumer_group,
        "client.id": "porterflow-raw-lander",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "enable.partition.eof": True,
        "max.poll.interval.ms": 900000,
        "session.timeout.ms": 45000,
    })
    assigned: set[tuple[str, int]] = set()
    at_eof: set[tuple[str, int]] = set()

    def on_assign(_c, partitions):
        assigned.update((p.topic, p.partition) for p in partitions)
        log.info("assigned %d partitions", len(partitions))

    def on_revoke(c, partitions):
        assigned.difference_update((p.topic, p.partition) for p in partitions)

    consumer.subscribe([settings.topic(s) for s in STREAMS], on_assign=on_assign, on_revoke=on_revoke)
    lander = RawLander(settings, run_date)
    started = last_msg = time.time()
    try:
        while time.time() - started < max_runtime_s:
            msgs = consumer.consume(num_messages=5000, timeout=1.0)
            for m in msgs:
                err = m.error()
                if err is not None:
                    if err.code() == KafkaError._PARTITION_EOF:
                        at_eof.add((m.topic(), m.partition()))
                        continue
                    raise KafkaException(err)
                at_eof.discard((m.topic(), m.partition()))
                lander.add(m)
                last_msg = time.time()
            if lander.buffered >= flush_every:
                lander.flush(consumer)
            if not assigned and time.time() - started > 90:
                raise TimeoutError("no partitions assigned after 90s: is the broker reachable?")
            caught_up = bool(assigned) and assigned <= at_eof
            idle = time.time() - last_msg > idle_timeout_s
            if (caught_up and not msgs) or (idle and assigned):
                break
        else:
            log.warning("max runtime reached; remaining messages will be picked up next run")
        lander.flush(consumer)
    finally:
        consumer.close()

    summary = {
        "run_date": run_date.isoformat(),
        "batches": lander.batches,
        **lander.totals,
        "seconds": round(time.time() - started, 1),
    }
    manifest_path = settings.run_dir(run_date) / "consume_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    attempts = []
    if manifest_path.exists():  # a retried attempt keeps what earlier attempts already landed
        attempts = json.loads(manifest_path.read_text()).get("attempts", [])
    attempts.append(summary)
    manifest = {
        "run_date": run_date.isoformat(),
        "batches": [b for a in attempts for b in a["batches"]],
        "valid": {k: sum(a["valid"][k] for a in attempts) for k in summary["valid"]},
        "quarantined": sum(a["quarantined"] for a in attempts),
        "files": sum(a["files"] for a in attempts),
        "attempts": attempts,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))
    log.info("consume summary: %s", {k: v for k, v in summary.items() if k != "batches"})
    return manifest
