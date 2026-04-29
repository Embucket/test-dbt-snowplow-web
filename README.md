# test-dbt-snowplow-web

Test harness that runs the official [`dbt-snowplow-web`](https://hub.getdbt.com/snowplow/snowplow_web/latest/)
package against [Rustice](../rustice) — a Snowflake-protocol query engine — using
the `dbt-snowflake` adapter.

It simulates a Snowplow RDB loader by ingesting one parquet file per cycle from
the public bucket `s3://embucket-testdata/snowplow/`. Each file in that prefix
is one batch produced by [`../snowplow-events-parquet`](../snowplow-events-parquet)
(`generate-snowplow-events`), named after the run's UTC timestamp
(`20260428T205543Z.parquet`). Sorting by key yields chronological order.

## Prerequisites

1. **Rustice running** on `localhost:3000` (separate concern, not handled here):
   ```
   docker run --name rustice --rm -p 3000:3000 embucket/rustice
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
4. **Parquet batches in S3**. Drive the generator with `S3_PARQUET_PREFIX`:
   ```
   cd ../snowplow-events-parquet
   S3_PARQUET_PREFIX=s3://embucket-testdata/snowplow ./scripts/run-batch.sh
   ```
   Each invocation writes one `<RUN_ID>.parquet` under that prefix. Rerun
   periodically (or via cron) to grow the input set.

## Cycle

```
./make.sh bootstrap     # one time: creates DB, schema, events table, resets state
./make.sh cycle         # tick 1: load oldest unprocessed parquet, dbt run (full build)
./make.sh cycle         # tick 2: load next parquet, dbt run (incremental only)
./make.sh cycle         # tick N: ...
```

`loader/state.json` tracks the last loaded S3 key. `./make.sh reset` drops the
snowplow_web output schemas and rewinds state to `null`.

When the bucket has no new files, `load-next` exits 0 with a "nothing to do"
message and `cycle` becomes idempotent.

## How loading works

`loader/load_next.py`:

1. Lists `s3://$S3_BUCKET/$S3_PREFIX/` anonymously via `boto3` (UNSIGNED).
2. Picks the smallest `*.parquet` key strictly greater than
   `state.last_loaded_key`.
3. Issues, against Rustice via the Snowflake protocol:

   ```sql
   COPY INTO public_snowplow_manifest.events
   FROM 's3://embucket-testdata/snowplow/<key>'
   FILE_FORMAT = (TYPE = PARQUET)
   MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
   ```

No storage integration / external volume is created — Rustice reads the public
bucket anonymously. If the COPY fails, fix Rustice's S3 client config; do not
fall back to local staging.

## Schema reconciliation

The generator emits the canonical Snowplow Analytics SDK `Event.toTsv` layout
(125 columns, with raw JSON `contexts` / `unstruct_event` / `derived_contexts`).
The events DDL in `loader/events_ddl.sql` mirrors the **post-RDB-loader** layout
(131 columns, with per-schema split-out context columns + `load_tstamp`).

`MATCH_BY_COLUMN_NAME` reconciles the two:

- **Source-only columns** (`contexts`, `unstruct_event`, `derived_contexts`) —
  silently ignored.
- **Target-only columns** (`load_tstamp`,
  `contexts_com_snowplowanalytics_snowplow_web_page_1`, the IAB / UA / YAUAA /
  consent / CWV split-outs) — stay NULL.

Because those split-out columns stay NULL, the corresponding optional features
of the dbt-snowplow-web package would produce empty / errorful output.
`dbt_project.yml` therefore disables them:

- `snowplow__enable_consent: false`
- `snowplow__enable_cwv: false`
- `snowplow__enable_iab: false`
- `snowplow__enable_ua: false`
- `snowplow__enable_yauaa: false`
- `snowplow__enable_load_tstamp: false`

The harness exercises the core models — `snowplow_web_sessions`,
`snowplow_web_page_views`, `snowplow_web_users`, and the manifest /
incremental machinery. That's the surface most relevant to validating
Rustice's Snowflake compatibility.

## How `dbt-snowplow-web` is wired

- Installed from dbt Hub via `packages.yml` (`snowplow/snowplow_web`).
- Configured via `vars.snowplow_web.*` in `dbt_project.yml`.
- The atomic events source is declared in `models/sources.yml` as
  `source('atomic', 'events')`, mapped to `public_snowplow_manifest.events` via
  `snowplow__atomic_schema` / `snowplow__events`.

## Verifying

After `./make.sh cycle`:

```sql
SELECT COUNT(*) FROM public_snowplow_manifest.events;
SELECT model, last_success
FROM public_snowplow_manifest_snowplow_manifest.snowplow_web_incremental_manifest;
SELECT COUNT(*) FROM public_snowplow_manifest_derived.snowplow_web_page_views;
SELECT COUNT(*) FROM public_snowplow_manifest_derived.snowplow_web_sessions;
```

`events` row count should grow by exactly the loaded file's row count each
tick; `last_success` per model should advance after every successful tick.

## Important variable

`snowplow__start_date` in `dbt_project.yml` (currently `2025-09-01`) must be ≤
the earliest `collector_tstamp` in the first parquet you load, otherwise the
package will skip all data. After the first `./make.sh cycle`, `load_next.py` prints
the recent `collector_tstamp` window so you can adjust the var if needed.
