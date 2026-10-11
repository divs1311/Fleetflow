"""Publish one ingest-day of simulated app events to Kafka.

This module plays the role of the driver and customer apps. It is part of the
*source*, not of the pipeline, which is why it is idempotent per ingest day via a
marker file: re-running the task does not re-send a whole day (a real app never
would). Individual duplicates still happen, injected by the simulator.

Producer settings: acks=all + enable.idempotence=true, so a broker retry cannot
create duplicates *inside Kafka*; duplicates created by the app (resends) are
handled downstream by deduplicating on event_id.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timezone

from confluent_kafka import KafkaException, Producer

from ..config import STREAMS, Settings
from ..simulator import SimulatorSource
from ..simulator.mess import to_messages
from .kafka_admin import ensure_topics

log = logging.getLogger(__name__)


def _marker(settings: Settings, ingest_day: date):
    return settings.state_dir / "source_emitted" / f"{ingest_day.isoformat()}.json"


def produce_day(settings: Settings, ingest_day: date, force: bool = False) -> dict:
    marker = _marker(settings, ingest_day)
    if marker.exists() and not force:
        log.info("source already emitted %s, skipping (use force=True to resend)", ingest_day)
        return {**json.loads(marker.read_text()), "skipped": True}

    ensure_topics(settings.kafka_bootstrap, settings.topic_partitions)
    source = SimulatorSource(settings.state_dir / "sim_cache", settings.sim_seed, settings.sim_trips_per_day)
    emission = source.emission(ingest_day)

    producer = Producer({
        "bootstrap.servers": settings.kafka_bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 50,
        "batch.size": 262144,
        "queue.buffering.max.messages": 500000,
        "client.id": "porterflow-app-simulator",
    })
    stats = {"delivered": 0, "failed": 0}

    def on_delivery(err, _msg):
        if err is not None:
            stats["failed"] += 1
            log.error("delivery failed: %s", err)
        else:
            stats["delivered"] += 1

    started = time.time()
    counts = {}
    for kind in STREAMS:
        topic = settings.topic(kind)
        messages = to_messages(emission[kind], kind)
        counts[topic] = len(messages)
        for key, value, event_type in messages:
            while True:
                try:
                    # No explicit timestamp: the broker stamps CreateTime=now. Using the simulated
                    # event time would make old (backfilled) messages eligible for retention deletion.
                    producer.produce(topic, key=key, value=value, on_delivery=on_delivery,
                                     headers=[("event_type", event_type.encode())])
                    break
                except BufferError:
                    producer.poll(0.5)  # local queue full: let it drain, then retry
            producer.poll(0)
    remaining = producer.flush(120)
    if remaining or stats["failed"]:
        raise KafkaException(f"{remaining} messages not flushed, {stats['failed']} delivery failures")

    result = {
        "ingest_day": ingest_day.isoformat(),
        "messages": counts,
        "delivered": stats["delivered"],
        "seconds": round(time.time() - started, 1),
        "emitted_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps(result, indent=2))
    log.info("produced %s", result)
    return result
