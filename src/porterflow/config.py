"""Runtime settings, read once from environment variables.

Every component (simulator, ingestion, Spark, dbt wrappers, dashboard) reads its
paths and endpoints from here, so the same code runs on a laptop
(`python -m porterflow.cli ...`) and inside the Airflow containers.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Stream name -> (Kafka topic, partition count). The partition count caps consumer
# parallelism; the message key (trip_id / driver_id) keeps one entity's events ordered.
STREAMS = {
    "trip_events": ("porter.trip-events.v1", 6),
    "driver_sessions": ("porter.driver-sessions.v1", 3),
}


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value in (None, "") else value


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    kafka_bootstrap: str
    consumer_group: str
    topic_prefix: str
    master_api_url: str
    master_api_key: str
    sim_seed: int
    sim_trips_per_day: int
    pipeline_start: date
    late_lookback_days: int
    export_target: str
    s3_endpoint_url: str | None
    s3_bucket: str
    spark_master: str
    dbt_project_dir: Path
    dbt_executable: str

    # ---- kafka ----------------------------------------------------------------
    def topic(self, stream: str) -> str:
        """Topic for a stream; KAFKA_TOPIC_PREFIX isolates environments (dev/staging/tests)."""
        return f"{self.topic_prefix}{STREAMS[stream][0]}"

    @property
    def topic_partitions(self) -> dict[str, int]:
        return {self.topic(stream): n for stream, (_, n) in STREAMS.items()}

    # ---- lake layout -------------------------------------------------------
    @property
    def lake_dir(self) -> Path:
        return self.data_dir / "lake"

    @property
    def raw_dir(self) -> Path:
        return self.lake_dir / "raw"

    def raw_table_dir(self, table: str) -> Path:
        return self.raw_dir / table

    @property
    def master_dir(self) -> Path:
        return self.raw_dir / "master"

    @property
    def quarantine_dir(self) -> Path:
        return self.lake_dir / "quarantine"

    @property
    def curated_dir(self) -> Path:
        return self.lake_dir / "curated"

    @property
    def utilisation_dir(self) -> Path:
        return self.curated_dir / "driver_hourly_utilisation"

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse" / "porterflow.duckdb"

    @property
    def serving_path(self) -> Path:
        return self.data_dir / "serving" / "porterflow_serving.duckdb"

    @property
    def state_dir(self) -> Path:
        return self.data_dir / "state"

    @property
    def quality_dir(self) -> Path:
        return self.data_dir / "quality"

    def run_dir(self, run_date: date) -> Path:
        return self.state_dir / "runs" / f"run_date={run_date.isoformat()}"


def load_settings() -> Settings:
    return Settings(
        data_dir=Path(_env("PORTERFLOW_DATA_DIR", str(PROJECT_ROOT / "data"))).resolve(),
        kafka_bootstrap=_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        consumer_group=_env("KAFKA_CONSUMER_GROUP", "porterflow-raw-lander"),
        topic_prefix=os.environ.get("KAFKA_TOPIC_PREFIX", ""),
        master_api_url=_env("MASTER_API_URL", "http://localhost:8000").rstrip("/"),
        master_api_key=_env("MASTER_API_KEY", "porterflow-dev-key"),
        sim_seed=int(_env("SIM_SEED", "42")),
        sim_trips_per_day=int(_env("SIM_TRIPS_PER_DAY", "20000")),
        pipeline_start=date.fromisoformat(_env("PORTERFLOW_START_DATE", "2026-09-01")),
        late_lookback_days=int(_env("PORTERFLOW_LATE_LOOKBACK_DAYS", "3")),
        export_target=_env("PORTERFLOW_EXPORT_TARGET", "none").lower(),
        s3_endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
        s3_bucket=_env("S3_BUCKET", "porterflow-gold"),
        spark_master=_env("SPARK_MASTER", "local[2]"),
        dbt_project_dir=Path(_env("DBT_PROJECT_DIR", str(PROJECT_ROOT / "dbt" / "porterflow"))).resolve(),
        dbt_executable=_env("DBT_EXECUTABLE", "dbt"),
    )
