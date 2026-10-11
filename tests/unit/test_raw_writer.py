from datetime import datetime

import pyarrow.parquet as pq

from porterflow.ingestion.raw_writer import write_partitioned
from porterflow.schemas import DRIVER_SESSION_SCHEMA


def _row(offset, partition=0, day="2026-09-01", city="BLR"):
    return {"event_id": f"e{offset}", "schema_version": 1, "event_type": "driver_login",
            "event_ts_utc": datetime(2026, 9, 1, 3), "produced_at_utc": None, "driver_id": "D1", "vehicle_id": "V1",
            "city_code": city, "lat": 1.0, "lng": 2.0, "_kafka_topic": "t", "_kafka_partition": partition,
            "_kafka_offset": offset, "_loaded_at_utc": datetime(2026, 10, 1), "_batch_id": "b1",
            "__event_date": day, "__city": city}


def test_hive_layout_and_offset_based_names(tmp_path):
    rows = [_row(5), _row(6), _row(9, partition=1), _row(7, city="MUM")]
    written = write_partitioned(rows, DRIVER_SESSION_SCHEMA, tmp_path, ("event_date", "city"))
    names = sorted(p["path"].replace(str(tmp_path) + "/", "") for p in written)
    assert names == [
        "event_date=2026-09-01/city=BLR/part-p000-o000000000005-000000000006.parquet",
        "event_date=2026-09-01/city=BLR/part-p001-o000000000009-000000000009.parquet",
        "event_date=2026-09-01/city=MUM/part-p000-o000000000007-000000000007.parquet",
    ]
    table = pq.read_table(tmp_path / names[0])
    assert table.schema.names == DRIVER_SESSION_SCHEMA.names  # partition values live in the path only
    assert table.num_rows == 2


def test_relanding_the_same_offsets_is_idempotent(tmp_path):
    rows = [_row(1), _row(2)]
    write_partitioned(rows, DRIVER_SESSION_SCHEMA, tmp_path, ("event_date", "city"))
    write_partitioned(rows, DRIVER_SESSION_SCHEMA, tmp_path, ("event_date", "city"))
    files = list(tmp_path.rglob("*.parquet"))
    assert len(files) == 1
    assert not list(tmp_path.rglob(".*.tmp"))  # temp files never left behind
