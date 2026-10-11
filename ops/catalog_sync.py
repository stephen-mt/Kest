#!/usr/bin/env python3
"""Synchronize OpenMetadata using isolated dbt and ingestion environments."""

from __future__ import annotations

import os
import subprocess
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INGESTION_PYTHON = "/opt/venvs/ingestion/bin/python"
DBT = "/opt/venvs/dbt/bin/dbt"
METADATA = "/opt/venvs/metadata/bin/metadata"


def run(*args: str, env: dict[str, str]) -> None:
    subprocess.run(args, check=True, cwd=ROOT, env=env)


def sync_catalog() -> None:
    try:
        with urllib.request.urlopen("http://openmetadata-server:8585/healthcheck", timeout=10) as response:
            if response.status != 200:
                raise RuntimeError(f"OpenMetadata healthcheck returned {response.status}")
    except OSError as error:
        raise RuntimeError("OpenMetadata is not running; start it with `make catalog-up`") from error

    token = subprocess.run(
        [INGESTION_PYTHON, str(ROOT / "ops/openmetadata_login.py")],
        check=True,
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=os.environ,
    ).stdout.strip()
    env = {
        **os.environ,
        "OPENMETADATA_INGESTION_TOKEN": token,
        "PYTHON_DOTENV_DISABLED": "1",
        "DBT_TARGET_PATH": "/tmp/kest-dbt-target",
        "DBT_LOG_PATH": "/tmp/kest-dbt-logs",
        "OPENMETADATA_DBT_ARTIFACT_DIR": "/tmp/kest-dbt-metadata",
    }
    run(
        DBT,
        "docs",
        "generate",
        "--project-dir",
        str(ROOT / "transformations"),
        "--profiles-dir",
        str(ROOT / "transformations"),
        "--target",
        "production",
        env=env,
    )
    run(INGESTION_PYTHON, str(ROOT / "ops/prepare_metadata_artifacts.py"), env=env)
    run(METADATA, "ingest", "-c", str(ROOT / "governance/openmetadata/clickhouse.yml"), env=env)
    run(INGESTION_PYTHON, str(ROOT / "ops/bootstrap_governance.py"), env=env)
    run(METADATA, "ingest", "-c", str(ROOT / "governance/openmetadata/dbt.yml"), env=env)
    run(
        METADATA,
        "ingest",
        "-c",
        str(ROOT / "governance/openmetadata/airflow.yml"),
        env={**env, "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "sqlite:////tmp/openmetadata-airflow.db"},
    )
    run(METADATA, "ingest", "-c", str(ROOT / "governance/openmetadata/superset.yml"), env=env)
    run(INGESTION_PYTHON, str(ROOT / "ops/verify_metadata.py"), env=env)


if __name__ == "__main__":
    sync_catalog()
