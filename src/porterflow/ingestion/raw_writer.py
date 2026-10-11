"""Write Parquet files into the lake atomically and idempotently.

* Atomic: write to a hidden temp file, then rename. Readers (DuckDB, Spark) never
  see half-written files, and both ignore names starting with '.'.
* Idempotent: file names are derived from the Kafka partition and offset range
  they contain, so re-landing the same messages overwrites the same file instead
  of adding a second copy.
"""
from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.parquet as pq


def write_table_atomic(table: pa.Table, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.tmp"
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, path)
    return path


def write_partitioned(rows: Iterable[dict], schema: pa.Schema, base_dir: Path,
                      partition_keys: tuple[str, ...]) -> list[dict]:
    """Group rows by (partition_keys..., kafka partition) and write one file per group.

    Partition values are taken from row fields named like `__<key>` (they are not
    stored inside the file, the directory carries them: hive-style partitioning).
    Returns a list of {path, rows} written.
    """
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        key = tuple(row[f"__{k}"] for k in partition_keys) + (row["_kafka_partition"],)
        groups[key].append(row)

    written = []
    names = schema.names
    for key, group in sorted(groups.items()):
        *part_values, kafka_partition = key
        offsets = [r["_kafka_offset"] for r in group]
        directory = base_dir.joinpath(*[f"{k}={v}" for k, v in zip(partition_keys, part_values)])
        name = f"part-p{kafka_partition:03d}-o{min(offsets):012d}-{max(offsets):012d}.parquet"
        table = pa.Table.from_pylist([{n: r.get(n) for n in names} for r in group], schema=schema)
        written.append({"path": str(write_table_atomic(table, directory / name)), "rows": len(group)})
    return written
