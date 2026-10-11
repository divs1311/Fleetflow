"""Topic management. Topics are created explicitly (never auto-created) so the
partition count, which fixes the maximum consumer parallelism and the ordering
guarantee per key, is a deliberate decision checked into the repo."""
from __future__ import annotations

import logging

from confluent_kafka.admin import AdminClient, NewTopic

log = logging.getLogger(__name__)

TOPIC_CONFIG = {
    "retention.ms": str(7 * 24 * 3600 * 1000),  # 7 days of replay buffer
    "cleanup.policy": "delete",
}


def ensure_topics(bootstrap: str, partitions: dict[str, int], timeout: float = 30.0) -> dict[str, int]:
    """Create missing topics; return {topic: partition_count} as seen by the broker."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=timeout).topics
    to_create = [NewTopic(t, num_partitions=n, replication_factor=1, config=TOPIC_CONFIG)
                 for t, n in partitions.items() if t not in existing]
    if to_create:
        for topic, fut in admin.create_topics(to_create, operation_timeout=timeout).items():
            try:
                fut.result()
                log.info("created topic %s", topic)
            except Exception as exc:  # another process may have created it concurrently
                if "TOPIC_ALREADY_EXISTS" not in str(exc) and "already exists" not in str(exc).lower():
                    raise
    md = admin.list_topics(timeout=timeout).topics
    found = {t: len(md[t].partitions) for t in partitions if t in md}
    missing = set(partitions) - set(found)
    if missing:
        raise RuntimeError(f"topics not visible after creation: {sorted(missing)}")
    return found
