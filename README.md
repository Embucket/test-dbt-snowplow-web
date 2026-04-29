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

## Dev mode (local filesystem)

For local iteration without S3 / network, set `DEV=1` and the loader reads
parquet from `$LOCAL_PARQUET_DIR` (default `./data/snowplow/`) and issues
`COPY INTO ... FROM 'file:///...'` instead of `s3://...`. How parquet files
land in that directory is out of scope for this harness — assume they're
already there.

Start Rustice with its iceberg-file-catalog rooted under `./data/catalog/`,
and bind-mount `./data` into the container at the *same host path* so the
absolute `file://` URLs the loader emits resolve identically inside the
container:

```
mkdir -p data/snowplow data/catalog
docker run --name rustice --rm -p 3000:3000 \
  -v "$(pwd)/data:$(pwd)/data:rw" \
  -e CATALOG_URL="file://$(pwd)/data/catalog" \
  -e BUCKET_HOST=0.0.0.0 \
  embucket/rustice
```

`CATALOG_URL` with a `file:` scheme switches Rustice into dev-catalog mode,
which is what enables `COPY INTO ... FROM 'file://...'`. The single `data/`
mount holds both the parquet inputs (`data/snowplow/`) and Rustice's
iceberg-catalog metadata (`data/catalog/`).

Then:

```
cp .env.example .env   # uncomment the DEV=1 / LOCAL_PARQUET_DIR lines
source .env
./make.sh bootstrap
./make.sh cycle
```

## Cycle

```
./make.sh bootstrap     # one time: creates DB, schema, events table, resets state
./make.sh cycle         # tick 1: load oldest unprocessed parquet, dbt run (full build)
./make.sh cycle         # tick 2: load next parquet, dbt run (incremental only)
./make.sh cycle         # tick N: ...
```

`loader/state.json` tracks the last loaded S3 key. `./make.sh reset` drops the
snowplow_web output schemas and rewinds state to `null`.

To force a full rebuild of every model (e.g. after a `reset`), pass
`--full-refresh` through to dbt:

```
./make.sh dbt-run --full-refresh   # or: ./make.sh cycle --full-refresh
```

Do **not** invoke `dbt run --full-refresh` directly — `dbt run` does not
materialize seeds, so the snowplow_web package's `*_dim_*` seeds (e.g.
`snowplow_web_dim_ga4_source_categories`) will be missing and downstream
models will fail with `table … not found`. The `dbt-run` target wraps
`dbt seed --full-refresh` + `dbt run` together so the seeds always exist.

When the bucket has no new files, `load-next` exits 0 with a "nothing to do"
message and `cycle` becomes idempotent.

For local iteration without S3, see [Dev mode](#dev-mode-local-filesystem).

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

- **Source-only columns** (`unstruct_event`, `derived_contexts`) — silently
  ignored.
- **`contexts_com_snowplowanalytics_snowplow_web_page_1`** — declared as
  `ARRAY(OBJECT(id VARCHAR))` on the events table (matches the parquet
  writer's `LIST<STRUCT<id:STRING>>` 1:1 and supports the `[0]:id::varchar`
  access path the dbt-snowplow-web package compiles against). The generator
  emits a `web_page` self-describing context for every page_view/page_ping (id
  derived from `(domain_sessionid, pageIdx ∈ [0..4])`, an approximation of the
  real Snowplow JS tracker which mints one UUID per page load and reuses it for
  subsequent pings — exact correlation isn't reproducible in our stateless
  generator). The parquet writer regex-extracts that id from the raw `contexts`
  JSON, and COPY INTO loads it via `MATCH_BY_COLUMN_NAME`.
- **Other target-only columns** (`load_tstamp`, the IAB / UA / YAUAA / consent
  / CWV split-outs) — stay NULL.

Because those remaining split-out columns stay NULL, the corresponding optional
features of the dbt-snowplow-web package would produce empty / errorful output.
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
