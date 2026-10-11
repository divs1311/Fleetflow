"""Publish: hand the finished marts to consumers.

1. Serving database (blue/green swap). The dashboard never reads the warehouse
   that dbt is writing to. Publish copies the marts into a brand-new DuckDB file
   and atomically renames it over the old one, so readers always see a complete,
   consistent day and never a half-built table.
2. Cloud export (optional, PORTERFLOW_EXPORT_TARGET=s3): every published table is
   written as Parquet to an S3 bucket (MinIO locally; AWS S3 + Athena or GCS +
   BigQuery external tables in a real deployment) with a manifest of row counts,
   then read back and verified.
"""
from __future__ import annotations

import io
import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .config import Settings

log = logging.getLogger(__name__)

PUBLISHED_TABLES = {
    "marts": ["dim_city", "dim_driver", "dim_vehicle", "dim_date", "fct_trips", "fct_driver_shifts",
              "agg_city_daily", "agg_driver_daily", "agg_cancellations", "agg_city_hourly",
              "fct_driver_hourly_utilisation"],
    "reference": ["cities", "vehicle_types", "cancellation_reasons"],
    "ops": ["dq_check_results"],
}


def build_serving_db(settings: Settings, run_date: date) -> dict:
    target = settings.serving_path
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.parent / f".{target.name}.building"
    for leftover in (tmp, Path(f"{tmp}.wal")):
        if leftover.exists():
            leftover.unlink()
    con = duckdb.connect(str(tmp))
    counts = {}
    try:
        con.execute(f"attach '{settings.warehouse_path}' as wh (read_only)")
        for schema, tables in PUBLISHED_TABLES.items():
            con.execute(f"create schema if not exists {schema}")
            for t in tables:
                con.execute(f"create table {schema}.{t} as select * from wh.{schema}.{t}")
                counts[f"{schema}.{t}"] = con.execute(f"select count(*) from {schema}.{t}").fetchone()[0]
        con.execute("create table ops.publish_log (run_date date, published_at_utc timestamp, tables json)")
        con.execute("insert into ops.publish_log values (?, ?, ?)",
                    [run_date, datetime.now(timezone.utc).replace(tzinfo=None), json.dumps(counts)])
        con.execute("detach wh")
        con.execute("checkpoint")
    finally:
        con.close()
    os.replace(tmp, target)  # atomic on POSIX: readers see the old file or the new one, never a mix
    log.info("serving database published: %s", counts)
    return {"run_date": run_date.isoformat(), "path": str(target), "tables": counts}


def export_to_s3(settings: Settings, run_date: date) -> dict:
    import boto3  # imported lazily: only needed when exporting

    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )
    bucket = settings.s3_bucket
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)

    con = duckdb.connect(str(settings.serving_path), read_only=True)
    manifest = {"run_date": run_date.isoformat(), "exported_at_utc": datetime.now(timezone.utc).isoformat(),
                "tables": {}}
    staging = settings.state_dir / "export"
    staging.mkdir(parents=True, exist_ok=True)
    try:
        for schema, tables in PUBLISHED_TABLES.items():
            for t in tables:
                local = staging / f"{t}.parquet"
                con.execute(f"copy (select * from {schema}.{t}) to '{local}' (format parquet, compression zstd)")
                rows = pq.ParquetFile(local).metadata.num_rows
                key = f"gold/{t}/{t}.parquet"
                s3.upload_file(str(local), bucket, key)
                # verify: download and compare the row count written into the Parquet footer
                body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
                remote_rows = pq.ParquetFile(io.BytesIO(body)).metadata.num_rows
                if remote_rows != rows:
                    raise RuntimeError(f"export verification failed for {t}: {rows} local vs {remote_rows} remote")
                manifest["tables"][t] = {"key": key, "rows": rows, "bytes": len(body)}
    finally:
        con.close()
    s3.put_object(Bucket=bucket, Key="gold/_manifest.json", Body=json.dumps(manifest, indent=2).encode(),
                  ContentType="application/json")
    log.info("exported %d tables to s3://%s/gold/", len(manifest["tables"]), bucket)
    return manifest


def export(settings: Settings, run_date: date) -> dict:
    if settings.export_target == "s3":
        return export_to_s3(settings, run_date)
    if settings.export_target in ("", "none"):
        log.info("PORTERFLOW_EXPORT_TARGET=none: skipping cloud export")
        return {"skipped": True}
    raise ValueError(f"unsupported PORTERFLOW_EXPORT_TARGET={settings.export_target!r} (use 's3' or 'none')")
