#!/usr/bin/env python3
"""Create a local recovery bundle without shell command interpolation."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime

from ops.platform_runtime import ROOT, compose, container_path, read_env


def main() -> None:
    values = read_env()
    backup_id = str(uuid.uuid4())
    created_at = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = ROOT / ".runtime" / "backups" / f"{created_at}-{backup_id}"
    postgres_dir = backup_dir / "postgres"
    postgres_dir.mkdir(mode=0o700, parents=True)
    backup_dir.chmod(0o700)

    for database in ("business_source", "platform_control", "airflow", "superset", "openmetadata"):
        with (postgres_dir / f"{database}.dump").open("wb") as output:
            compose(
                "exec",
                "-T",
                "postgres",
                "pg_dump",
                "--username",
                "postgres",
                "--format",
                "custom",
                database,
                stdout=output,
            )

    compose(
        "run",
        "--rm",
        "tooling",
        "python",
        "/opt/kest/ops/recovery.py",
        "backup-raw",
        container_path(backup_dir),
    )
    clickhouse_path = f"{backup_id}/clickhouse"
    backup_query = (
        "BACKUP DATABASE raw, DATABASE gold "
        f"TO S3('http://object-store:9000/platform-backups/{clickhouse_path}', "
        f"'{values['S3_BACKUP_ACCESS_KEY']}', '{values['S3_BACKUP_SECRET_KEY']}')"
    )
    compose(
        "exec",
        "-T",
        "clickhouse",
        "clickhouse-client",
        "--user",
        "default",
        "--password",
        values["CLICKHOUSE_ADMIN_PASSWORD"],
        "--query",
        backup_query,
    )
    compose(
        "run",
        "--rm",
        "tooling",
        "python",
        "/opt/kest/ops/recovery.py",
        "write-checksums",
        container_path(backup_dir),
    )
    artifact_checksum = hashlib.sha256((backup_dir / "SHA256SUMS").read_bytes()).hexdigest()
    (backup_dir / "backup.json").write_text(
        json.dumps(
            {
                "backup_id": backup_id,
                "created_at": created_at,
                "clickhouse_path": clickhouse_path,
                "artifact_checksum": artifact_checksum,
            },
            indent=2,
        )
        + "\n"
    )
    (ROOT / ".runtime" / "backups" / "LATEST").write_text(f"{backup_dir}\n")
    sql_path = str(backup_dir).replace("'", "''")
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
        f"INSERT INTO backup_manifest(backup_id,path,checksum) VALUES ('{backup_id}','{sql_path}','{artifact_checksum}')",
    )
    print(f"Backup complete: {backup_dir}")
    print("Copy this directory off-host for host-loss recovery.")


if __name__ == "__main__":
    main()
