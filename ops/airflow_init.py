#!/usr/bin/env python3
"""Idempotently initialize Airflow metadata, identities, pools and DAG state."""

from __future__ import annotations

import os
import subprocess


def airflow(*args: str, check: bool = True) -> None:
    subprocess.run(["airflow", *args], check=check)


def main() -> None:
    airflow("db", "migrate")
    if os.getenv("KEST_AUTH_MODE", "oidc") != "oidc":
        airflow(
            "users", "create",
            "--username", os.environ["AIRFLOW_ADMIN_USER"],
            "--password", os.environ["AIRFLOW_ADMIN_PASSWORD"],
            "--firstname", "Kest", "--lastname", "Admin",
            "--role", "Admin", "--email", "airflow@kest.local",
            check=False,
        )
    pools = {
        "source_ingestion": ("3", "Source API and database extraction"),
        "clickhouse_transform": ("2", "ClickHouse and dbt transformations"),
        "gold_publish": ("1", "Serialized Gold publication"),
        "heavy_backfill": ("1", "Resource-capped historical rebuilds"),
        "catalog_sync": ("1", "OpenMetadata synchronization"),
    }
    for name, (slots, description) in pools.items():
        airflow("pools", "set", name, slots, description)
    airflow("dags", "reserialize")
    dags = subprocess.run(
        ["airflow", "dags", "list", "--output", "json"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if "backup_restore_verify" in dags:
        airflow("dags", "delete", "backup_restore_verify", "--yes")
    for dag_id in ("source_ingestion", "transform_publish", "controlled_backfill", "ecommerce_end_to_end"):
        airflow("dags", "unpause", dag_id)
    airflow("dags", "pause", "metadata_sync", check=False)


if __name__ == "__main__":
    main()
