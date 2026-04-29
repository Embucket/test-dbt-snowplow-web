#!/usr/bin/env python3
"""Drop snowplow_web output schemas and reset state.json.

Used by `make reset` between full test runs.
"""

import json
import sys
from pathlib import Path

from _connection import connect

DATABASE = "embucket"
SCHEMAS = [
    "public_snowplow_manifest_derived",
    "public_snowplow_manifest_scratch",
    "public_snowplow_manifest_snowplow_manifest",
    "public_snowplow_manifest",
]
STATE_PATH = Path(__file__).parent / "state.json"


def main():
    conn = connect()
    cur = conn.cursor()
    cur.execute(f"USE DATABASE {DATABASE}")
    for schema in SCHEMAS:
        print(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        try:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        except Exception as exc:
            print(f"  warning: {exc}", file=sys.stderr)
    cur.close()
    conn.close()

    STATE_PATH.write_text(json.dumps({"next_index": 1}) + "\n")
    print(f"Reset {STATE_PATH.name} -> next_index=1")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"reset failed: {exc}", file=sys.stderr)
        sys.exit(1)
