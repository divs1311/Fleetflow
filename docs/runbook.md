# Runbook

## Start, stop, reset

| Task | Command |
|---|---|
| Start everything (first time builds images) | `make up` |
| Follow the scheduler | `make logs` |
| See DAG run states | `make status` |
| Stop (keep data) | `make down` |
| Wipe everything (data, topics, Airflow metadata) | `make reset` |

The first `make up` backfills `PORTERFLOW_BACKFILL_DAYS` days, one run at a time. Watch it in Airflow
(http://localhost:8080 → `porterflow_daily` → Grid).

## A quality gate failed

1. Open the failed task's log in Airflow: every check prints `PASS/WARN/FAIL observed=... (threshold)`.
2. The same results are in `data/quality/run_date=<day>/<layer>_checks.json` and `ops.dq_check_results`.
3. Fix the cause (upstream bug, threshold too strict), then `make rerun DAY=<day>`.
   Nothing was published for that day, so the dashboard still shows the last good day.

## Reprocess a range (backfill)

```bash
make backfill START=2026-09-03 END=2026-09-05
```
Safe because every task is idempotent for its date.

## Change transformation logic

Incremental models only process new rows. After changing `int_*` or `fct_trips` logic:
```bash
make full-refresh DAY=2026-09-14    # triggers a run with {"full_refresh": true}
```
dbt then rebuilds the incremental models from the whole raw lake.

## Replay from Kafka

Raw Parquet is the source of truth, so replaying Kafka is rarely needed. If raw files were lost and the
messages are still within retention (7 days), reset the consumer group and re-run ingestion:
```bash
docker compose exec redpanda rpk group seek porterflow-raw-lander --to start
make rerun DAY=<day>
```

## Inspect data by hand

```bash
make shell
python - <<'EOF'
import duckdb
con = duckdb.connect("/opt/porterflow/data/serving/porterflow_serving.duckdb", read_only=True)
print(con.sql("select * from marts.agg_city_daily order by booked_date_ist desc limit 6"))
EOF
```
Or query the raw lake directly:
`select count(*) from read_parquet('/opt/porterflow/data/lake/raw/trip_events/*/*/*.parquet', hive_partitioning=true)`.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Containers exit with code 137 | Docker needs ≥ 6 GB of memory (Docker Desktop → Settings → Resources). |
| `permission denied` under `./data` on Linux | Set `AIRFLOW_UID=$(id -u)` in `.env` (`make env` does it), then `make reset && make up`. |
| Port already in use | Change the left side of the port mapping in `docker-compose.yml` (e.g. `"8081:8080"`). |
| Dashboard says "No published data yet" | The first DAG run has not finished; check Airflow. |
| Spark task fails with `JAVA_HOME` | Rebuild the image: `docker compose build --no-cache airflow-scheduler`. |
