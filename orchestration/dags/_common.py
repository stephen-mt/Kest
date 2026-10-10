from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator

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


def cli_task(task_id: str, command: str, pool: str, timeout_minutes: int = 30) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"{INGESTION_PYTHON} -m kest_ingestion.cli {command}",
        pool=pool,
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=timeout_minutes),
        on_failure_callback=failure_callback,
        do_xcom_push=False,
    )
