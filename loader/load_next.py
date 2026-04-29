#!/usr/bin/env python3
"""Load the next parquet file from s3://embucket-test/data/snowplow/ into events.

Each invocation:
  1. reads loader/state.json to get the next index N
  2. issues COPY INTO public_snowplow_manifest.events FROM 's3://.../events_NNN.parquet'
  3. prints row delta and min/max collector_tstamp
  4. advances state.json to N+1

Exits non-zero (without advancing state) if the COPY fails or finds no rows,
so `make cycle` halts cleanly when the input is exhausted.
"""

import json
import sys
from pathlib import Path

from _connection import connect

S3_PREFIX = "s3://embucket-test/data/snowplow"
DATABASE = "embucket"
SCHEMA = "public_snowplow_manifest"
TABLE = "events"

STATE_PATH = Path(__file__).parent / "state.json"


def main():
    state = json.loads(STATE_PATH.read_text())
    n = int(state["next_index"])
    filename = f"events_{n:03d}.parquet"
    url = f"{S3_PREFIX}/{filename}"

    conn = connect()
    cur = conn.cursor()
    cur.execute(f"USE DATABASE {DATABASE}")
    cur.execute(f"USE SCHEMA {SCHEMA}")

    cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
    (before,) = cur.fetchone()

    copy_sql = (
        f"COPY INTO {TABLE} FROM '{url}' "
        "FILE_FORMAT = (TYPE = PARQUET) "
        "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
    )
    print(f"Loading file #{n}: {url}")
    print(f"  {copy_sql}")
    cur.execute(copy_sql)

    cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
    (after,) = cur.fetchone()
    delta = after - before
    print(f"  rows: {before} -> {after}  (delta {delta:+})")

    if delta <= 0:
        print(f"  refusing to advance state: no new rows for {filename}", file=sys.stderr)
        sys.exit(2)

    cur.execute(
        f"SELECT MIN(collector_tstamp), MAX(collector_tstamp) FROM {TABLE} "
        f"WHERE load_tstamp = (SELECT MAX(load_tstamp) FROM {TABLE})"
    )
    row = cur.fetchone()
    if row and row[0] is not None:
        print(f"  this batch collector_tstamp range: {row[0]} -> {row[1]}")

    STATE_PATH.write_text(json.dumps({"next_index": n + 1}) + "\n")
    print(f"  state.json -> next_index={n + 1}")

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
