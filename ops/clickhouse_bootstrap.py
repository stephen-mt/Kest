#!/usr/bin/env python3
"""Apply idempotent ClickHouse SQL through the official client."""

from __future__ import annotations

from ops.platform_runtime import ROOT, compose, read_env


def main() -> None:
    with (ROOT / "infra/clickhouse/init/10-raw-tables.sql").open("rb") as sql:
        compose(
            "exec",
            "-T",
            "clickhouse",
            "clickhouse-client",
            "--user",
            "default",
            "--password",
            read_env()["CLICKHOUSE_ADMIN_PASSWORD"],
            "--multiquery",
            stdin=sql,
        )


if __name__ == "__main__":
    main()
