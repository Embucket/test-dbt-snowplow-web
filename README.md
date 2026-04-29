# test-dbt-snowplow-web

Test harness that runs the official [`dbt-snowplow-web`](https://hub.getdbt.com/snowplow/snowplow_web/latest/)
package against [Rustice](../rustice) — a Snowflake-protocol query engine — using
the `dbt-snowflake` adapter.

It simulates a Snowplow RDB loader by ingesting one parquet file per cycle from
the public bucket `s3://embucket-test/data/snowplow/` (files numbered
`events_001.parquet`, `events_002.parquet`, …). Each cycle runs `dbt run`, so
the package's incremental models are exercised the same way they would be in
production.

## Prerequisites

1. **Rustice running** on `localhost:3000` (separate concern, not handled here):
   ```
   docker run --name rustice --rm -p 3000:3000 embucket/embucket
   ```
2. **Python venv** with the pinned deps:
   ```
   python3 -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. **Environment variables**:
   ```
   cp .env.example .env && source .env
   ```

## Cycle

```
make bootstrap     # one time: creates DB, schema, events table, resets state.json
make cycle         # tick 1: load events_001.parquet, dbt run (full build)
make cycle         # tick 2: load events_002.parquet, dbt run (incremental only)
make cycle         # tick N: ...
```

`loader/state.json` tracks which file goes next. `make reset` drops all
snowplow_web output schemas and rewinds the counter to 1.

## How loading works

`loader/load_next.py` issues, against Rustice via the Snowflake protocol:

```sql
COPY INTO public_snowplow_manifest.events
FROM 's3://embucket-test/data/snowplow/events_NNN.parquet'
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

No storage integration / external volume is created — Rustice reads the public
bucket anonymously. If the COPY fails, fix Rustice's S3 client config; do not
fall back to local staging.

## How `dbt-snowplow-web` is wired

- Installed from dbt Hub via `packages.yml` (`snowplow/snowplow_web`).
- Configured via `vars.snowplow_web.*` in `dbt_project.yml`.
- The atomic events source is declared in `models/sources.yml` as
  `source('atomic', 'events')`, mapped to `public_snowplow_manifest.events` via
  `snowplow__atomic_schema` / `snowplow__events`.

## Verifying

After `make cycle`:

```sql
SELECT COUNT(*) FROM public_snowplow_manifest.events;
SELECT model, last_success
FROM public_snowplow_manifest_snowplow_manifest.snowplow_web_incremental_manifest;
SELECT COUNT(*) FROM public_snowplow_manifest_derived.snowplow_web_page_views;
SELECT COUNT(*) FROM public_snowplow_manifest_derived.snowplow_web_sessions;
```

`events` row count should grow by exactly file N's row count each tick;
`last_success` per model should advance after every successful tick.

## Important variable

`snowplow__start_date` in `dbt_project.yml` must be ≤ the earliest
`collector_tstamp` in `events_001.parquet`, otherwise the package will skip all
data. After the first `make cycle`, `load_next.py` prints the batch's
`collector_tstamp` range so you can adjust the var if needed.
