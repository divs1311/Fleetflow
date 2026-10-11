"""The pipeline's tasks as plain functions of (settings, run_date).

Airflow (dags/porterflow_daily.py) and the CLI (porterflow.cli) both call these,
so orchestration stays a thin layer and every task can be run and tested alone.
Every task is idempotent for a given run_date.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import date, timedelta

from .config import Settings, load_settings

log = logging.getLogger(__name__)


def simulate_source(settings: Settings, run_date: date, force: bool = False) -> dict:
    from .ingestion.producer import produce_day
    return produce_day(settings, run_date, force=force)


def ingest_stream(settings: Settings, run_date: date) -> dict:
    from .ingestion.consumer import consume_to_raw
    return consume_to_raw(settings, run_date)


def ingest_master_data(settings: Settings, run_date: date) -> dict:
    from .ingestion.master_data import extract_master_data
    return extract_master_data(settings, run_date)


def raw_quality_gate(settings: Settings, run_date: date) -> list:
    from .quality.checks import gate
    return [r.__dict__ for r in gate(settings, run_date, "raw")]


def spark_utilisation(settings: Settings, run_date: date) -> dict:
    from .spark_jobs.driver_utilisation import run
    return run(run_date, settings.data_dir, settings.spark_master, settings.late_lookback_days,
               plan_dir=settings.state_dir / "spark_plans", history_start=settings.pipeline_start)


def dbt_env(settings: Settings) -> dict:
    env = dict(os.environ)
    env.update({
        "PORTERFLOW_DATA_DIR": str(settings.data_dir),
        "PORTERFLOW_WAREHOUSE_PATH": str(settings.warehouse_path),
        "DBT_PROFILES_DIR": str(settings.dbt_project_dir),
        "DBT_TARGET_PATH": str(settings.state_dir / "dbt" / "target"),
        "DBT_LOG_PATH": str(settings.state_dir / "dbt" / "logs"),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    })
    return env


def dbt(settings: Settings, run_date: date, command: str = "build", full_refresh: bool = False,
        extra: list[str] | None = None) -> None:
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    args = [settings.dbt_executable, command, "--project-dir", str(settings.dbt_project_dir),
            "--vars", json.dumps({"run_date": run_date.isoformat()})]
    if full_refresh:
        args.append("--full-refresh")
    args += extra or []
    log.info("running %s", " ".join(args))
    subprocess.run(args, env=dbt_env(settings), check=True)


def warehouse_quality_gate(settings: Settings, run_date: date) -> list:
    from .quality.checks import gate
    return [r.__dict__ for r in gate(settings, run_date, "warehouse")]


def publish(settings: Settings, run_date: date) -> dict:
    from .publish import build_serving_db
    return build_serving_db(settings, run_date)


def export_cloud(settings: Settings, run_date: date) -> dict:
    from .publish import export
    return export(settings, run_date)


PIPELINE = [
    ("simulate_source", simulate_source),
    ("ingest_stream", ingest_stream),
    ("ingest_master_data", ingest_master_data),
    ("raw_quality_gate", raw_quality_gate),
    ("spark_utilisation", spark_utilisation),
    ("dbt_build", dbt),
    ("warehouse_quality_gate", warehouse_quality_gate),
    ("publish", publish),
    ("export_cloud", export_cloud),
]


def run_day(run_date: date, settings: Settings | None = None, skip: set[str] | None = None) -> None:
    settings = settings or load_settings()
    for name, fn in PIPELINE:
        if skip and name in skip:
            continue
        log.info("=== %s %s ===", run_date, name)
        fn(settings, run_date)


def run_range(start: date, end: date, settings: Settings | None = None, skip: set[str] | None = None) -> None:
    day = start
    while day <= end:
        run_day(day, settings, skip)
        day += timedelta(days=1)
