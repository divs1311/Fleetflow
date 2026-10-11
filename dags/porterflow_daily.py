"""
### PorterFlow daily pipeline

One run = one IST business day (`ds`).

```
simulate_source ─> consume_stream_to_raw ─┐
                   extract_master_data ───┴─> raw_quality_gate ─> spark_driver_utilisation
   ─> dbt_build (seed + run + snapshot + test) ─> warehouse_quality_gate ─> publish_serving ─> export_to_cloud
```

* **Idempotent**: every task can be re-run for the same `ds` without double counting
  (deterministic raw file names, event_id dedup, delete+insert on trip_id, partition
  overwrite in Spark, atomic swap of the serving database).
* **Backfill**: `catchup=True` with `max_active_runs=1` replays every day from the start
  date in order. Re-process a range with
  `airflow dags backfill porterflow_daily -s 2026-09-03 -e 2026-09-05 --reset-dagruns`.
* **Quality gates** never retry: a failing check is a data problem, not a transient error.
  Use the `full_refresh` param to rebuild incremental models from raw after a logic change.
"""
from __future__ import annotations

import os
from datetime import date, timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

START = pendulum.parse(os.environ.get("PORTERFLOW_START_DATE", "2026-09-01"), tz="UTC")
BACKFILL_DAYS = int(os.environ.get("PORTERFLOW_BACKFILL_DAYS", "14"))
END = START.add(days=BACKFILL_DAYS - 1)
DBT = os.environ.get("DBT_EXECUTABLE", "dbt")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/porterflow/dbt/porterflow")
SPARK_JOB = os.environ.get("PORTERFLOW_SPARK_JOB", "/opt/porterflow/src/porterflow/spark_jobs/driver_utilisation.py")


def run_task(task_name: str, ds: str, **_) -> object:
    """Call a framework-agnostic task from porterflow.tasks (imported lazily: fast DAG parsing)."""
    from porterflow import tasks
    from porterflow.config import load_settings

    return dict(tasks.PIPELINE)[task_name](load_settings(), date.fromisoformat(ds))


def py_task(task_id: str, task_name: str, **kwargs) -> PythonOperator:
    return PythonOperator(task_id=task_id, python_callable=run_task, op_kwargs={"task_name": task_name}, **kwargs)


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="porterflow_daily",
    description="Trips + driver events -> Kafka -> Parquet lake -> Spark + dbt star schema -> dashboard",
    doc_md=__doc__,
    schedule="@daily",
    start_date=START,
    end_date=END,
    catchup=True,
    max_active_runs=1,       # DuckDB has a single writer and incremental models assume day order
    max_active_tasks=4,
    default_args=default_args,
    params={"full_refresh": Param(False, type="boolean", description="Rebuild incremental dbt models from raw")},
    tags=["porterflow", "logistics", "elt"],
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup("ingest", tooltip="Streaming + batch ingestion into the raw zone") as ingest:
        simulate_source = py_task("simulate_source", "simulate_source",
                                  doc_md="Stands in for the driver/customer apps: publishes one day of events to Kafka.")
        consume = py_task("consume_stream_to_raw", "ingest_stream",
                          doc_md="Micro-batch consumer: Kafka -> partitioned Parquet, commits offsets after write.")
        master = py_task("extract_master_data", "ingest_master_data",
                         doc_md="Incremental REST extract of drivers and vehicles for the run's data interval.")
        simulate_source >> consume

    raw_gate = py_task("raw_quality_gate", "raw_quality_gate", retries=0)

    spark = BashOperator(
        task_id="spark_driver_utilisation",
        bash_command=(f"spark-submit --master \"${{SPARK_MASTER:-local[2]}}\" --driver-memory 2g "
                      f"{SPARK_JOB} --run-date {{{{ ds }}}}"),
        append_env=True,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"{DBT} build --project-dir {DBT_PROJECT_DIR} --vars '{{\"run_date\": \"{{{{ ds }}}}\"}}' "
            "{{ '--full-refresh' if params.full_refresh else '' }}"
        ),
        append_env=True,
        doc_md="Seeds, staging views, incremental models, SCD2 snapshot, marts and every dbt test, in DAG order.",
    )

    warehouse_gate = py_task("warehouse_quality_gate", "warehouse_quality_gate", retries=0)
    publish = py_task("publish_serving", "publish")
    export = py_task("export_to_cloud", "export_cloud")
    done = EmptyOperator(task_id="done")

    start >> ingest >> raw_gate >> spark >> dbt_build >> warehouse_gate >> publish >> export >> done
