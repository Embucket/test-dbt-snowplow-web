"""Shared snowflake.connector helper for the loader scripts."""

import os
import sys

import snowflake.connector


def connect():
    host = os.environ.get("EMBUCKET_HOST", "localhost")
    port = int(os.environ.get("EMBUCKET_PORT", "3000"))
    protocol = os.environ.get("EMBUCKET_PROTOCOL", "http")
    account = os.environ.get("EMBUCKET_ACCOUNT", "test")
    user = os.environ.get("EMBUCKET_USER", "embucket")
    password = os.environ.get("EMBUCKET_PASSWORD", "embucket")
    database = os.environ.get("EMBUCKET_DATABASE", "embucket")
    warehouse = os.environ.get("EMBUCKET_WAREHOUSE", "COMPUTE_WH")

    print(f"Connecting to Rustice at {protocol}://{host}:{port} (account={account})", file=sys.stderr)
    return snowflake.connector.connect(
        host=host,
        port=port,
        protocol=protocol,
        account=account,
        user=user,
        password=password,
        database=database,
        warehouse=warehouse,
    )
