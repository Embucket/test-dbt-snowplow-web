#!/usr/bin/env bash

PYTHON="${PYTHON:-python3}"
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$(pwd)}"

help() {
  echo "Targets:"
  echo "  bootstrap   - create database, schema, events table, fresh state.json"
  echo "  deps        - dbt deps (installs snowplow_web from Hub)"
  echo "  load-next   - COPY INTO events from the next events_NNN.parquet on S3"
  echo "  dbt-run     - dbt deps + seed + run + test"
  echo "  cycle       - load-next, then dbt-run (one simulated loader tick)"
  echo "  reset       - drop derived/scratch/manifest schemas and reset state"
  echo "  clean       - remove dbt target/ and dbt_packages/"
}

bootstrap() {
  "$PYTHON" loader/bootstrap.py
}

deps() {
  dbt deps
}

load-next() {
  "$PYTHON" loader/load_next.py
}

dbt-run() {
  deps
  dbt seed --full-refresh
  dbt run 2>&1 | tee dbt_output.log
  dbt test || true
}

cycle() {
  load-next
  dbt-run
}

reset() {
  "$PYTHON" loader/reset.py
}

clean() {
  rm -rf target dbt_packages
}

"$1" "${2:0}"

