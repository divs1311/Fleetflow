# Cloud: publishing the gold tables

Every successful run exports the published tables as Parquet to an S3-compatible bucket:

```
s3://porterflow-gold/gold/<table>/<table>.parquet
s3://porterflow-gold/gold/_manifest.json        # run_date, row count and size of every table
```

Locally the bucket lives in **MinIO** (http://localhost:9001, `porterflow` / `porterflow-secret`). The
export reads every file back and compares its row count with the source before writing the manifest.

## Option A: AWS S3 + Athena

1. Create a bucket and an IAM user with `s3:PutObject/GetObject/ListBucket` on it.
2. In `.env`: `PORTERFLOW_EXPORT_TARGET=s3`, and in `docker-compose.yml` remove `S3_ENDPOINT_URL`
   (empty = real AWS), set `S3_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`.
3. In Athena:

```sql
CREATE EXTERNAL TABLE porterflow.agg_city_daily (
  booked_date_ist date, city_code string, city_name string, trips_booked bigint, trips_completed bigint,
  trips_cancelled bigint, trips_in_progress bigint, completion_rate double, cancellation_rate double,
  cancelled_by_customer bigint, cancelled_by_driver bigint, cancelled_by_system bigint,
  cancelled_before_assignment bigint, gmv_inr double, avg_fare_inr double, median_time_to_assign_sec double,
  avg_dead_km double, dead_km_share double, avg_trip_km double, drivers_with_trips bigint,
  drivers_online bigint, fleet_utilisation double, speed_anomalies bigint, late_event_share double)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/gold/agg_city_daily/';
```
(Column types must match the Parquet file; check with `duckdb -c "describe select * from 'agg_city_daily.parquet'"`.)

## Option B: BigQuery sandbox (free, no credit card)

1. Create a project at https://console.cloud.google.com/bigquery (the sandbox is enabled automatically)
   and a dataset `porterflow`.
2. Install the Google Cloud CLI, then `gcloud auth login`.
3. Load the exported Parquet files (download them from MinIO, or use `data/state/export/` after a run):

```bash
scripts/publish_bigquery.sh <your-gcp-project> porterflow data/state/export
```

The script runs `bq load --replace --source_format=PARQUET` for every table, so re-running it is idempotent.
BigQuery infers the schema from Parquet. Sandbox tables expire after 60 days.

## Warehouse vs lake, in one sentence

The lake (S3 + Parquet) is cheap, open storage any engine can read; the warehouse (BigQuery, or DuckDB
here) is where modelled, tested tables are served fast to analysts. PorterFlow keeps raw and curated data in
the lake and publishes the gold tables to both.
