"""Reusable Airflow 3 Task SDK definitions for Kest DAGs."""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import timedelta

from airflow.sdk import task
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig

INGESTION_PYTHON = "/opt/venvs/ingestion/bin/python"
DBT_EXECUTABLE = "/opt/venvs/dbt/bin/dbt"
DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30),
}


def failure_callback(context: dict) -> None:
    message = {
        "text": (
            f"Kest task failure: dag={context['dag'].dag_id} "
            f"task={context['task_instance'].task_id} run={context['run_id']}"
        )
    }
    webhook = os.getenv("PLATFORM_ALERT_WEBHOOK_URL")
    if not webhook:
        logging.error("PLATFORM_ALERT_WEBHOOK_URL is not configured; alert=%s", message["text"])
        return
    request = urllib.request.Request(
        webhook,
        data=json.dumps(message).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=10).close()
    except Exception:
        logging.exception("Failure webhook delivery failed")


def task_options(pool: str, timeout_minutes: int = 30) -> dict:
    return {
        "pool": pool,
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=timeout_minutes),
        "on_failure_callback": failure_callback,
    }


@task.external_python(python=INGESTION_PYTHON, expect_airflow=False, do_xcom_push=False)
def ingest_sources_task() -> None:
    from kest_ingestion.config import Settings, require_secrets
    from kest_ingestion.workflows import ingest_sources

    settings = Settings()
    require_secrets(settings)
    print({"batch_ids": ingest_sources(settings)})


@task.external_python(python=INGESTION_PYTHON, expect_airflow=False, do_xcom_push=False)
def load_raw_task() -> None:
    from kest_ingestion.config import Settings, require_secrets
    from kest_ingestion.workflows import load_raw

    settings = Settings()
    require_secrets(settings)
    print({"loaded_batch_ids": load_raw(settings)})


@task.external_python(python=INGESTION_PYTHON, expect_airflow=False, do_xcom_push=False)
def reconcile_sources_task() -> None:
    from kest_ingestion.config import Settings, require_secrets
    from kest_ingestion.workflows import reconcile_sources

    settings = Settings()
    require_secrets(settings)
    print({"tombstone_batch_id": reconcile_sources(settings)})


@task.external_python(python=INGESTION_PYTHON, expect_airflow=False, do_xcom_push=False)
def publish_gold_task() -> None:
    from kest_ingestion.config import Settings, require_secrets
    from kest_ingestion.workflows import publish_gold

    settings = Settings()
    require_secrets(settings)
    print({"publications": publish_gold(settings)})


@task.external_python(python=INGESTION_PYTHON, expect_airflow=False, do_xcom_push=False)
def business_smoke_task() -> None:
    from kest_ingestion.config import Settings, require_secrets
    from kest_ingestion.workflows import smoke

    settings = Settings()
    require_secrets(settings)
    print(smoke(settings))


@task.external_python(python=INGESTION_PYTHON, expect_airflow=False, do_xcom_push=False)
def run_backfill_task(source: str, start: str, end: str, reason: str, dry_run: bool) -> None:
    from kest_ingestion.config import Settings, require_secrets
    from kest_ingestion.workflows import run_backfill, validate_backfill

    plan = validate_backfill(source, start, end, reason)
    if dry_run:
        print({"dry_run": True, **plan})
        return
    settings = Settings()
    require_secrets(settings)
    print(run_backfill(settings, source, start, end, reason))


@task(
    pool="catalog_sync",
    retries=1,
    execution_timeout=timedelta(minutes=30),
    do_xcom_push=False,
)
def catalog_sync_task() -> None:
    from ops.catalog_sync import sync_catalog

    sync_catalog()


def dbt_candidate_group(group_id: str = "dbt_build_candidate") -> DbtTaskGroup:
    return DbtTaskGroup(
        group_id=group_id,
        project_config=ProjectConfig("/opt/kest/transformations"),
        profile_config=ProfileConfig(
            profile_name="kest",
            target_name="production",
            profiles_yml_filepath="/opt/kest/transformations/profiles.yml",
        ),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
        operator_args={
            **task_options("clickhouse_transform", 45),
            "install_deps": False,
        },
    )
