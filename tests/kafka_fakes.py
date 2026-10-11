"""In-memory stand-ins for confluent_kafka objects, so the landing logic can be
tested (and CI can run end to end) without a broker. The real broker path is
covered by tests/integration/test_kafka_roundtrip.py when KAFKA_BOOTSTRAP_SERVERS
points at a running Kafka/Redpanda."""
from __future__ import annotations

import zlib
from datetime import date

from porterflow.config import STREAMS, Settings
from porterflow.ingestion.consumer import RawLander
from porterflow.simulator import SimulatorSource
from porterflow.simulator.mess import to_messages


class FakeMessage:
    def __init__(self, topic, partition, offset, key, value):
        self._t, self._p, self._o, self._k, self._v = topic, partition, offset, key, value

    def topic(self):
        return self._t

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def key(self):
        return self._k

    def value(self):
        return self._v

    def error(self):
        return None


class FakeConsumer:
    def __init__(self):
        self.committed = {}

    def commit(self, offsets, asynchronous=False):
        for tp in offsets:
            self.committed[(tp.topic, tp.partition)] = tp.offset


class FakeLog:
    """A tiny Kafka: per-partition append-only logs with offsets, keyed partitioning."""

    def __init__(self, settings: Settings):
        self.partitions = settings.topic_partitions
        self.logs = {(t, p): [] for t, n in self.partitions.items() for p in range(n)}

    def produce(self, topic, key, value):
        p = zlib.crc32(key) % self.partitions[topic]
        self.logs[(topic, p)].append((key, value))

    def read_from(self, committed):
        for (t, p), entries in self.logs.items():
            for off in range(committed.get((t, p), 0), len(entries)):
                k, v = entries[off]
                yield FakeMessage(t, p, off, k, v)


def emit_day(log: FakeLog, settings: Settings, day: date) -> None:
    src = SimulatorSource(settings.state_dir / "sim_cache", settings.sim_seed, settings.sim_trips_per_day)
    em = src.emission(day)
    for kind in STREAMS:
        for key, value, _ in to_messages(em[kind], kind):
            log.produce(settings.topic(kind), key, value)


def land(log: FakeLog, consumer: FakeConsumer, settings: Settings, run_date: date, flush_every=10**9) -> dict:
    """Same RawLander as production, fed from the fake log."""
    import json
    lander = RawLander(settings, run_date)
    for msg in log.read_from(consumer.committed):
        lander.add(msg)
        if lander.buffered >= flush_every:
            lander.flush(consumer)
    lander.flush(consumer)
    manifest = {"run_date": run_date.isoformat(), "batches": lander.batches, **lander.totals, "attempts": []}
    path = settings.run_dir(run_date) / "consume_manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest))
    return manifest
