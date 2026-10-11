"""PySpark job: hourly driver utilisation across all trips.

    spark-submit src/porterflow/spark_jobs/driver_utilisation.py --run-date 2026-09-05

utilisation(driver, hour) = seconds busy on a trip / seconds logged in, per IST hour.

Design notes (the parts interviewers ask about):
  * Reads only the date partitions it needs (partition pruning on event_date).
  * Recomputes a sliding window of days [run_date - lookback, run_date] on every
    run and overwrites exactly those output partitions (dynamic partition
    overwrite). Late events inside the window are absorbed; re-runs are idempotent.
  * Wide transformations (dropDuplicates, groupBy, window) cause shuffles; the
    shuffle partition count is sized for the data instead of the default 200.
  * The small driver dimension is broadcast to avoid shuffling the large side.
  * Time math is done in epoch seconds with an explicit +05:30 offset, so results
    never depend on the cluster's timezone.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

IST_OFFSET_S = 19800
MAX_SHIFT_S = 14 * 3600

log = logging.getLogger("driver_utilisation")


def build_spark(master: str, shuffle_partitions: int = 8) -> SparkSession:
    return (
        SparkSession.builder.appName("porterflow-driver-hourly-utilisation")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def _epoch(col: str) -> F.Column:
    # event_ts_utc is a UTC wall-clock timestamp; with session timezone UTC the cast is exact.
    return F.col(col).cast("timestamp").cast("double")


def read_events(spark: SparkSession, base: Path, first: date, last: date) -> DataFrame:
    return (
        spark.read.option("basePath", str(base)).parquet(str(base))
        .where(F.col("event_date").between(F.lit(first.isoformat()), F.lit(last.isoformat())))
    )


def hour_overlaps(intervals: DataFrame, key_cols: list[str]) -> DataFrame:
    """Explode [start_s, end_s) intervals into IST hour buckets with the overlapping seconds."""
    hour_idx = F.floor((F.col("start_s") + IST_OFFSET_S) / 3600).cast("long")
    last_idx = F.floor((F.col("end_s") - 0.001 + IST_OFFSET_S) / 3600).cast("long")
    exploded = intervals.where(F.col("end_s") > F.col("start_s")).withColumn(
        "hour_idx", F.explode(F.sequence(hour_idx, last_idx)))
    h_start = F.col("hour_idx") * 3600 - IST_OFFSET_S
    overlap = F.least(F.col("end_s"), h_start + 3600) - F.greatest(F.col("start_s"), h_start)
    return exploded.select(*key_cols, "hour_idx", overlap.alias("overlap_s"))


def compute(spark: SparkSession, raw_dir: Path, master_dir: Path, run_date: date, lookback_days: int,
            history_start: date | None = None) -> DataFrame:
    # recompute the last `lookback_days` days, but never days before data collection began
    out_first = max(run_date - timedelta(days=lookback_days), history_start or date.min)
    # one extra day on each side: trips/shifts crossing midnight, and a logout that already landed for a re-run
    read_first, read_last = out_first - timedelta(days=1), run_date + timedelta(days=1)
    window_end_s = F.lit(float((run_date - date(1970, 1, 1)).days + 1) * 86400 - IST_OFFSET_S)

    trips_raw = read_events(spark, raw_dir / "trip_events", read_first, read_last).dropDuplicates(["event_id"])
    sessions_raw = read_events(spark, raw_dir / "driver_sessions", read_first, read_last).dropDuplicates(["event_id"])

    # ---- busy intervals: assignment -> delivery / cancellation (open trips capped at window end)
    trips = (
        trips_raw.where(F.col("event_type").isin("driver_assigned", "trip_delivered", "trip_cancelled"))
        .groupBy("trip_id")
        .agg(
            F.min(F.when(F.col("event_type") == "driver_assigned", _epoch("event_ts_utc"))).alias("start_s"),
            F.min(F.when(F.col("event_type") != "driver_assigned", _epoch("event_ts_utc"))).alias("end_s"),
            F.first(F.when(F.col("event_type") == "driver_assigned", F.col("driver_id")), ignorenulls=True)
            .alias("driver_id"),
        )
        .where(F.col("start_s").isNotNull() & F.col("driver_id").isNotNull())
        .withColumn("end_s", F.least(F.coalesce(F.col("end_s"), window_end_s), F.col("start_s") + MAX_SHIFT_S))
    )
    busy = (
        hour_overlaps(trips, ["driver_id"])
        .groupBy("driver_id", "hour_idx")
        .agg(F.sum("overlap_s").alias("busy_seconds"))
    )
    trips_started = (
        trips.withColumn("hour_idx", F.floor((F.col("start_s") + IST_OFFSET_S) / 3600).cast("long"))
        .groupBy("driver_id", "hour_idx").agg(F.count("*").alias("trips_started"))
    )

    # ---- online intervals: login -> next logout (missing logout capped at 14h / window end)
    w = Window.partitionBy("driver_id").orderBy("ts_s", "event_type")
    sessions = (
        sessions_raw.select("driver_id", "city_code", "event_type", _epoch("event_ts_utc").alias("ts_s"))
        .withColumn("next_type", F.lead("event_type").over(w))
        .withColumn("next_ts", F.lead("ts_s").over(w))
        .where(F.col("event_type") == "driver_login")
        .select(
            "driver_id", "city_code", F.col("ts_s").alias("start_s"),
            F.when(F.col("next_type") == "driver_logout", F.col("next_ts"))
            .otherwise(F.least(F.col("ts_s") + MAX_SHIFT_S, window_end_s)).alias("end_s"),
        )
    )
    online = (
        hour_overlaps(sessions, ["driver_id", "city_code"])
        .groupBy("driver_id", "city_code", "hour_idx")
        .agg(F.sum("overlap_s").alias("online_seconds"))
    )

    # ---- small dimension, broadcast: latest vehicle type per driver from the master extracts
    drivers = (
        spark.read.parquet(str(master_dir / "drivers"))
        .withColumn("rn", F.row_number().over(Window.partitionBy("driver_id").orderBy(F.desc("updated_at_utc"))))
        .where("rn = 1")
        .select("driver_id", F.col("vehicle_type"), F.col("city_code").alias("master_city_code"))
    )

    hourly = (
        online.join(busy, ["driver_id", "hour_idx"], "full_outer")
        .join(trips_started, ["driver_id", "hour_idx"], "left")
        .join(F.broadcast(drivers), "driver_id", "left")
        .select(
            "driver_id",
            F.coalesce("city_code", "master_city_code").alias("city_code"),
            "vehicle_type",
            F.date_format(F.timestamp_seconds(F.col("hour_idx") * 3600), "yyyy-MM-dd").alias("event_date"),
            (F.col("hour_idx") % 24).cast("int").alias("hour_of_day"),
            F.round(F.coalesce("online_seconds", F.lit(0.0)), 1).alias("online_seconds"),
            F.round(F.coalesce("busy_seconds", F.lit(0.0)), 1).alias("busy_seconds"),
            F.coalesce("trips_started", F.lit(0)).cast("int").alias("trips_started"),
        )
        .withColumn("utilisation", F.when(F.col("online_seconds") > 0,
                                          F.round(F.least(F.lit(1.0), F.col("busy_seconds") / F.col("online_seconds")), 4)))
        # busy time with no login recorded: a data quality signal, kept rather than dropped
        .withColumn("is_busy_outside_shift", F.col("busy_seconds") > F.col("online_seconds") + 60)
        .where(F.col("event_date").between(out_first.isoformat(), run_date.isoformat()))
        .where(F.col("city_code").isNotNull())
    )
    return hourly


def run(run_date: date, data_dir: Path, master: str, lookback_days: int, plan_dir: Path | None = None,
        history_start: date | None = None) -> dict:
    raw_dir = data_dir / "lake" / "raw"
    out_dir = data_dir / "lake" / "curated" / "driver_hourly_utilisation"
    spark = build_spark(master)
    spark.sparkContext.setLogLevel("WARN")
    try:
        hourly = compute(spark, raw_dir, raw_dir / "master", run_date, lookback_days, history_start)
        if plan_dir is not None:
            plan_dir.mkdir(parents=True, exist_ok=True)
            plan = hourly._jdf.queryExecution().toString()
            (plan_dir / f"driver_utilisation_{run_date.isoformat()}.txt").write_text(plan)
        # few, reasonably sized files per output partition
        (hourly.repartition("event_date", "city_code")
         .withColumnRenamed("city_code", "city")
         .withColumn("city_code", F.col("city"))
         .write.mode("overwrite").partitionBy("event_date", "city").parquet(str(out_dir)))
        stats = (spark.read.parquet(str(out_dir))
                 .where(F.col("event_date").between((run_date - timedelta(days=lookback_days)).isoformat(),
                                                    run_date.isoformat()))
                 .agg(F.count("*").alias("rows"), F.countDistinct("driver_id").alias("drivers"),
                      F.avg("utilisation").alias("avg_utilisation"))
                 .collect()[0].asDict())
        log.info("driver utilisation written: %s", stats)
        return {"run_date": run_date.isoformat(), "lookback_days": lookback_days, **stats}
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--run-date", required=True, type=date.fromisoformat)
    p.add_argument("--data-dir", type=Path, default=Path(os.environ.get("PORTERFLOW_DATA_DIR", "data")))
    p.add_argument("--master", default=os.environ.get("SPARK_MASTER", "local[2]"))
    p.add_argument("--lookback-days", type=int, default=int(os.environ.get("PORTERFLOW_LATE_LOOKBACK_DAYS", "3")))
    p.add_argument("--history-start", type=date.fromisoformat,
                   default=date.fromisoformat(os.environ.get("PORTERFLOW_START_DATE", "2026-09-01")))
    a = p.parse_args(argv)
    result = run(a.run_date, a.data_dir.resolve(), a.master, a.lookback_days,
                 plan_dir=a.data_dir.resolve() / "state" / "spark_plans", history_start=a.history_start)
    print(result)


if __name__ == "__main__":
    sys.exit(main())
