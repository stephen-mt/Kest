#!/usr/bin/env python3
"""Restore the latest bundle into isolated targets and compare outcomes."""

from __future__ import annotations

import json

from ops.platform_runtime import ROOT, compose, container_path, read_env


def scalar(*args: str) -> str:
    return compose(*args, capture_output=True).stdout.decode().strip()


def main() -> None:
    values = read_env()
    latest_file = ROOT / ".runtime" / "backups" / "LATEST"
    if not latest_file.is_file():
        raise SystemExit("No backup found; run `make backup` first")
    backup_dir = latest_file.read_text().splitlines()[0]
    backup_path = ROOT / ".runtime" / "backups" / backup_dir.rsplit("/", 1)[-1]
    if not backup_path.is_dir():
        raise SystemExit(f"Backup directory is missing: {backup_path}")
    compose(
        "run",
        "--rm",
        "tooling",
        "python",
        "/opt/kest/ops/recovery.py",
        "verify-checksums",
        container_path(backup_path),
    )
    manifest = json.loads((backup_path / "backup.json").read_text())
    backup_id = manifest["backup_id"]
    suffix = backup_id.replace("-", "")[:10]

    for database in ("business_source", "platform_control"):
        target = f"restore_{database}_{suffix}"
        compose(
            "exec",
            "-T",
            "postgres",
            "psql",
            "--username",
            "postgres",
            "--dbname",
            "postgres",
            "--set=ON_ERROR_STOP=1",
            "--command",
            f"DROP DATABASE IF EXISTS {target} WITH (FORCE)",
            "--command",
            f"CREATE DATABASE {target}",
        )
        with (backup_path / "postgres" / f"{database}.dump").open("rb") as source:
            compose(
                "exec",
                "-T",
                "postgres",
                "pg_restore",
                "--username",
                "postgres",
                "--dbname",
                target,
                "--no-owner",
                "--clean",
                "--if-exists",
                stdin=source,
            )

    count_query = (
        "SELECT (SELECT count(*) FROM customers)+(SELECT count(*) FROM orders)+"
        "(SELECT count(*) FROM order_items)+(SELECT count(*) FROM payments)+"
        "(SELECT count(*) FROM refunds)+(SELECT count(*) FROM events)"
    )
    source_rows = scalar(
        "exec", "-T", "postgres", "psql", "-At", "--username", "postgres", "--dbname", "business_source", "--command", count_query
    )
    restore_rows = scalar(
        "exec", "-T", "postgres", "psql", "-At", "--username", "postgres", "--dbname", f"restore_business_source_{suffix}", "--command", count_query
    )
    if source_rows != restore_rows:
        raise RuntimeError(f"PostgreSQL row count mismatch: source={source_rows} restore={restore_rows}")

    compose(
        "run",
        "--rm",
        "tooling",
        "python",
        "/opt/kest/ops/recovery.py",
        "restore-raw",
        container_path(backup_path),
        "restore-verify",
    )
    compose("--profile", "recovery", "up", "-d", "--wait", "clickhouse-restore")
    restore_query = (
        "DROP DATABASE IF EXISTS restore_raw; DROP DATABASE IF EXISTS restore_gold; "
        "RESTORE DATABASE raw AS restore_raw, DATABASE gold AS restore_gold "
        f"FROM S3('http://object-store:9000/platform-backups/{manifest['clickhouse_path']}', "
        f"'{values['S3_BACKUP_ACCESS_KEY']}', '{values['S3_BACKUP_SECRET_KEY']}')"
    )
    compose(
        "exec",
        "-T",
        "clickhouse-restore",
        "clickhouse-client",
        "--user",
        "default",
        "--password",
        values["CLICKHOUSE_ADMIN_PASSWORD"],
        "--multiquery",
        "--query",
        restore_query,
    )
    for table in ("mart_daily_revenue", "mart_conversion_funnel", "mart_customer_orders"):
        checksum = "SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM {}.{} FORMAT TSV"
        common = ("clickhouse-client", "--user", "default", "--password", values["CLICKHOUSE_ADMIN_PASSWORD"], "--query")
        source_sum = scalar("exec", "-T", "clickhouse", *common, checksum.format("gold", table))
        restore_sum = scalar("exec", "-T", "clickhouse-restore", *common, checksum.format("restore_gold", table))
        if source_sum != restore_sum:
            raise RuntimeError(f"ClickHouse checksum mismatch for {table}")
    compose(
        "exec",
        "-T",
        "postgres",
        "psql",
        "--username",
        "postgres",
        "--dbname",
        "platform_control",
        "--set=ON_ERROR_STOP=1",
        "--command",
        f"UPDATE backup_manifest SET verified_at=now() WHERE backup_id='{backup_id}'",
    )
    print(
        f"Restore verified: backup={backup_id} postgres_rows={restore_rows} "
        "bucket=restore-verify ClickHouse=dedicated-instance"
    )


if __name__ == "__main__":
    main()
