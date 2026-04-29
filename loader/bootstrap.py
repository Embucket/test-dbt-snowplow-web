#!/usr/bin/env python3
"""One-shot setup: database, schema, events table, fresh state.json."""

import json
import os
import sys
from pathlib import Path

from _connection import connect

HERE = Path(__file__).parent
DDL_PATH = HERE / "events_ddl.sql"
STATE_PATH = HERE / "state.json"
DATABASE = "embucket"
SCHEMA = "public_snowplow_manifest"

DEV = os.environ.get("DEV", "").lower() in ("1", "true", "yes")


def split_statements(sql: str):
    parts, buf = [], []
    for line in sql.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        buf.append(line)
        if stripped.endswith(";"):
            parts.append("\n".join(buf).rstrip(";").strip())
            buf = []
    if buf:
        parts.append("\n".join(buf).strip())
    return [p for p in parts if p]


def main():
    conn = connect()
    cur = conn.cursor()

    if DEV:
        # Dev-mode rustice (file/s3 catalog) auto-registers the default
        # database and rejects CREATE DATABASE.
        print(f"USE DATABASE {DATABASE} (DEV mode; CREATE DATABASE skipped)")
    else:
        print(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    cur.execute(f"USE DATABASE {DATABASE}")

    print(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    cur.execute(f"USE SCHEMA {SCHEMA}")

    print(f"Applying DDL from {DDL_PATH.name}")
    for stmt in split_statements(DDL_PATH.read_text()):
        cur.execute(stmt)

    cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.events")
    (count,) = cur.fetchone()
    print(f"  events table ready, current row count = {count}")

    STATE_PATH.write_text(json.dumps({"last_loaded_key": None}) + "\n")
    print(f"Reset {STATE_PATH.name} -> last_loaded_key=null")

    cur.close()
    conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"bootstrap failed: {exc}", file=sys.stderr)
        sys.exit(1)
