#!/usr/bin/env python3
"""Load the next unprocessed parquet file from the public Snowplow bucket.

The generate-snowplow-events project (see ../snowplow-events-parquet) writes one
parquet per batch when invoked with `S3_PARQUET_PREFIX=s3://embucket-testdata/snowplow`.
The filename is the run's UTC timestamp, e.g. `20260428T205543Z.parquet`. Sorting
keys lexicographically therefore yields chronological order.

Each invocation:
  1. Lists `s3://$S3_BUCKET/$S3_PREFIX/` anonymously via boto3 UNSIGNED.
  2. Picks the smallest *.parquet key strictly greater than state.last_loaded_key.
  3. Issues `COPY INTO public_snowplow_manifest.events FROM 's3://...'`.
  4. Persists the loaded key into state.json.

Exits 0 with a "no new files" message (without advancing state) if the bucket
holds nothing newer; this lets `make cycle` be idempotent at the end of input.
Exits non-zero on actual failures.
"""

import json
import os
import sys
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from _connection import connect

S3_BUCKET = os.environ.get("S3_BUCKET", "embucket-testdata")
S3_PREFIX = os.environ.get("S3_PREFIX", "snowplow/").lstrip("/")
S3_REGION = os.environ.get("S3_REGION", "us-east-2")

DEV = os.environ.get("DEV", "").lower() in ("1", "true", "yes")
LOCAL_PARQUET_DIR = os.environ.get("LOCAL_PARQUET_DIR", "./data/snowplow")

DATABASE = "embucket"
SCHEMA = "public_snowplow_manifest"
TABLE = "events"

STATE_PATH = Path(__file__).parent / "state.json"


def list_parquet_keys_s3() -> list[str]:
    """List every *.parquet object under s3://$S3_BUCKET/$S3_PREFIX, sorted."""
    s3 = boto3.client(
        "s3",
        region_name=S3_REGION,
        config=Config(signature_version=UNSIGNED),
    )
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []) or []:
            key = obj["Key"]
            if key.endswith(".parquet"):
                keys.append(key)
    keys.sort()
    return keys


def list_parquet_keys_local() -> list[str]:
    """List every *.parquet basename under LOCAL_PARQUET_DIR, sorted."""
    return sorted(p.name for p in Path(LOCAL_PARQUET_DIR).glob("*.parquet"))


def list_parquet_keys() -> list[str]:
    return list_parquet_keys_local() if DEV else list_parquet_keys_s3()


def main():
    state = json.loads(STATE_PATH.read_text()) if STATE_PATH.exists() else {}
    last_key = state.get("last_loaded_key") or ""

    if DEV:
        local_dir = Path(LOCAL_PARQUET_DIR).resolve()
        print(f"Listing {local_dir} (DEV mode, file:// backend)")
    else:
        print(f"Listing s3://{S3_BUCKET}/{S3_PREFIX} (anonymous)")
    keys = list_parquet_keys()
    print(f"  found {len(keys)} parquet object(s)")

    candidates = [k for k in keys if k > last_key]
    if not candidates:
        print(f"  no new files since last_loaded_key={last_key!r}; nothing to do")
        return

    next_key = candidates[0]
    if DEV:
        url = f"file://{Path(LOCAL_PARQUET_DIR).resolve() / next_key}"
    else:
        url = f"s3://{S3_BUCKET}/{next_key}"
    print(f"Next file: {url}")

    conn = connect()
    cur = conn.cursor()
    cur.execute(f"USE DATABASE {DATABASE}")
    cur.execute(f"USE SCHEMA {SCHEMA}")

    cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
    (before,) = cur.fetchone()

    # MATCH_BY_COLUMN_NAME aligns parquet columns with target columns by name.
    # Source-only columns (contexts / unstruct_event / derived_contexts in the
    # generator output) are ignored; target-only columns (the per-schema
    # context split-outs and load_tstamp) stay NULL — the dbt-snowplow-web
    # optional features that read them are disabled in dbt_project.yml.
    copy_sql = (
        f"COPY INTO {TABLE} FROM '{url}' "
        "FILE_FORMAT = (TYPE = 'PARQUET') "
        "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
    )
    print(f"  {copy_sql}")
    cur.execute(copy_sql)

    cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
    (after,) = cur.fetchone()
    delta = after - before
    print(f"  rows: {before} -> {after}  (delta {delta:+})")

    if delta <= 0:
        print(f"  refusing to advance state: no new rows for {next_key}", file=sys.stderr)
        sys.exit(2)

    cur.execute(
        f"SELECT MIN(collector_tstamp), MAX(collector_tstamp) "
        f"FROM {TABLE} "
        f"WHERE collector_tstamp >= (SELECT MAX(collector_tstamp) - INTERVAL '1' DAY FROM {TABLE})"
    )
    row = cur.fetchone()
    if row and row[0] is not None:
        print(f"  recent collector_tstamp window: {row[0]} -> {row[1]}")

    state["last_loaded_key"] = next_key
    STATE_PATH.write_text(json.dumps(state) + "\n")
    print(f"  state.json -> last_loaded_key={next_key!r}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        print(f"load_next failed: {exc}", file=sys.stderr)
        sys.exit(1)
