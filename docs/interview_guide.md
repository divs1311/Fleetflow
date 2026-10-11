# Interview guide: questions this project answers

Each answer points to the file that proves it. Say the answer in your own words, then open the file.

---

### SQL

**1. Deduplicate trip events, keeping one record per event.**
`dbt/porterflow/models/intermediate/int_trip_events.sql`
```sql
select * from new_rows
qualify row_number() over (partition by event_id order by _loaded_at_utc, _kafka_partition, _kafka_offset) = 1
```
plus `where not exists (select 1 from {{ this }} t where t.event_id = f.event_id)` so an event already in
the table is never inserted again on a later run. Dedup on `event_id` (the event), not `trip_id`: a trip
legitimately has 3-4 events. The "latest record per trip" version of the question is `fct_trips`, which
collapses all events of a trip into one row with `min(...) filter (where event_type = ...)` per stage.

**2. Window functions: running totals, ranking drivers per city.**
`fct_driver_shifts.sql` pairs each login with the next event using `lead() over (partition by driver_id
order by event_ts_utc)`; `int_drivers_current.sql` keeps the latest version with `row_number()`. Ranking
drivers per city by trips is one line on `agg_driver_daily`:
`rank() over (partition by city_code, activity_date_ist order by trips_completed desc)`.

**3. Join types, and when a join silently duplicates rows.**
The `fct_trips → dim_driver` join is the example: `dim_driver` has several rows per driver (SCD2). Joining on
`driver_id` alone would multiply every trip by the number of versions. The fix is joining at the right grain
(`assigned_at_utc` within `[valid_from, valid_to)`), and the test that proves it:
`tests/singular/assert_scd2_no_overlapping_versions.sql` plus `unique` on `fct_trips.trip_id`.
`agg_driver_daily` uses a FULL OUTER JOIN between shifts and trips because a driver can be online without a
trip, or (data issue) have a trip without a recorded login.

### Data modelling

**4. Design a schema for trips, drivers and vehicles. Star or snowflake?**
Star: `fct_trips` + `dim_driver` (SCD2) + `dim_vehicle` (SCD1) + `dim_city` + `dim_date`
(`docs/architecture.md` §6). Star because analysts and BI tools write simpler, faster queries with one join
per dimension; vehicle-type attributes are denormalised into `dim_vehicle` instead of a separate
`dim_vehicle_type` (that would be snowflaking).

**5. What is the grain of your fact table and why?**
One row per booked trip, an *accumulating snapshot*: each lifecycle stage is a timestamp column that fills in
as events arrive. Trip-level grain matches the business questions (completion rate, time to assign, dead
km per trip). The event-level grain is kept in `int_trip_events` for anyone who needs it.

### ETL vs ELT

**6. Why load raw first and transform in the warehouse?**
Raw Parquet is the immutable source of truth; every transformation is SQL in git, tested by dbt, and can be
re-run from raw (`--full-refresh`) after a logic change or a bug, without re-ingesting from Kafka (which
only retains 7 days). It also keeps ingestion simple and fast.

### History and late data

**7. A driver changes vehicle. How do you keep history?**
SCD Type 2 via a dbt snapshot (`snapshots/snap_drivers.sql`, timestamp strategy on the source's
`updated_at`). The old row gets a `valid_to`; a new row starts. Facts join as of the event time. Type 1
would overwrite (lose history); Type 3 keeps only the previous value in an extra column.

**8. An event arrives 2 days late. How does your pipeline handle it?**
It lands in its correct (old) date partition; `int_trip_events` picks it up because incremental filtering
is on *load* time; `fct_trips` recomputes that trip from all its events (delete+insert on `trip_id`); Spark
recomputes the last 3 days each run. Proven by `tests/integration::test_late_events_are_absorbed`: >95%
of trips that were `in_progress` after day 1 settle by day 4.

### Orchestration

**9. If the DAG re-runs for the same day, do you double-count?**
No. Raw file names come from Kafka offsets (rewrite, not append), dedup on `event_id`, delete+insert on
`trip_id`, partition overwrite in Spark, the master extract window is fixed by the run date, and the
serving DB is rebuilt then swapped. Test: `test_rerunning_a_day_is_idempotent`.

**10. How do you safely reprocess the last 30 days?**
`airflow dags backfill porterflow_daily -s <start> -e <end> --reset-dagruns` (`make backfill`). Because
tasks are idempotent and `max_active_runs=1` keeps days in order, a backfill is just "run those days again".
After a transformation logic change, trigger with `{"full_refresh": true}` to rebuild incremental models
from raw.

**11. How do retries, dependencies and scheduling work in your DAG?**
`dags/porterflow_daily.py`: `@daily` schedule, `catchup=True`, `start_date`/`end_date`, `max_active_runs=1`.
Default retries=2 with exponential backoff for transient failures (broker down, API 503). Quality gates
have `retries=0`: bad data is not transient. Dependencies are explicit with `>>` and a TaskGroup for
ingestion.

### Kafka

**12. Topics, partitions, offsets; at-least-once vs exactly-once?**
Two topics; `trip-events` has 6 partitions keyed by `trip_id`, so all events of one trip are ordered within
one partition, and up to 6 consumers can share the load. The consumer commits offsets only after the Parquet
file is renamed into place: at-least-once. Exactly-once in Kafka needs idempotent producers + transactions +
`read_committed` consumers *and* an idempotent sink; I get the same end result with deterministic file
names + dedup on `event_id` (`ingestion/consumer.py`).

**13. When would Porter need streaming instead of daily batch?**
Streaming for decisions that expire in seconds or minutes: live trip tracking, dispatch, surge pricing,
fraud. Daily batch for reporting: the KPIs in this dashboard are fine a few hours late. That is why the
events go through Kafka (available in real time to anyone) while the warehouse is loaded in batches.

### Spark

**14. What causes a shuffle and how do you reduce it?**
Wide transformations: `dropDuplicates`, `groupBy`, window functions and joins on non-co-partitioned
keys all move rows across the network so equal keys meet. In `spark_jobs/driver_utilisation.py`:
partition pruning reads only 5 days; the small driver dimension is `broadcast` (no shuffle of the large
side); `spark.sql.shuffle.partitions` is sized for the data (8 locally, not the default 200); AQE coalesces
small partitions. The physical plan of each run is saved to `data/state/spark_plans/` to point at the
`Exchange` (shuffle) nodes.

### File formats

**15. Why Parquet over CSV? Why partition by date and city?**
Columnar (read 3 columns of 30), compressed (zstd), typed (no re-parsing), with min/max statistics for
predicate pushdown. Partitioning by `event_date` and `city` matches the filters almost every query uses, so
engines skip whole folders. Don't partition by high-cardinality keys (trip_id): millions of tiny files.

### Data quality

**16. How do you stop bad data reaching dashboards?**
Four layers (`docs/architecture.md` §7): the contract quarantines bad messages; the raw gate blocks the run on
abnormal duplicate/null/late rates; 91 dbt tests run inside `dbt build` and block downstream models; the
warehouse gate reconciles row counts raw → intermediate → fact → KPI and blocks publishing. The serving DB
is only swapped after all gates pass, so the dashboard always shows the last *good* day.

### Cloud

**17. Warehouse vs data lake: what did you put where?**
Lake (Parquet on disk / S3): raw and curated data, cheap, any engine can read it. Warehouse (DuckDB here,
BigQuery in production): modelled, tested tables for analytics. Gold tables are exported to S3
(MinIO locally) as Parquet with a manifest, queryable by Athena or as BigQuery external tables
(`docs/cloud.md`).

### Python

**18. Write a function to parse and validate a batch of events.**
`schemas.validate_trip_event` (required fields, enums, ISO timestamps with timezone, coordinate ranges, type
checks) and `ingestion/consumer.parse_message`, which turns any failure into a quarantine record instead of
crashing the batch. Unit tests: `tests/unit/test_schemas.py`.

### Business sense

**19. Which metric would you show Porter's ops team first and why?**
Completion rate with cancellations split by stage and owner. In this data most lost trips are *before
assignment* (no driver found / long wait) and concentrate in the evening peak, a supply problem the supply
team can act on (shift incentives 17:00-21:00), not a product problem. Dead km per trip is the second: it is
pure cost (fuel, driver time) and dispatch logic can reduce it.

---

## Numbers to quote (reference run: 14 days, 20,000 trips/day, seed 42)

| Metric | Value |
|---|---|
| Kafka messages through the pipeline | 1.22 M (318 quarantined, 0 lost) |
| Duplicates removed by dedup | 16,200 |
| Trips in the fact table | 293,923 |
| Trip status vs simulator truth | 99.97% match |
| Automated quality checks | 115 (91 dbt tests + 24 gate checks), 0 failures in 14 runs |
| Completion rate / cancellations before assignment | 78.8% / 53% of cancellations |
| Dead km per completed trip | 4.0 km (31% of all km) |
| Runtime | ~1 min per day under Airflow |

Re-run the repo and use your own numbers if you change any setting.
