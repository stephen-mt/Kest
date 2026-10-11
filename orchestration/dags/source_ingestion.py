from datetime import datetime

from _common import (
    DEFAULT_ARGS,
    ingest_sources_task,
    load_raw_task,
    reconcile_sources_task,
    task_options,
)
from airflow.sdk import DAG

with DAG(
    dag_id="source_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["kest", "ingestion"],
) as dag:
    ingest = ingest_sources_task.override(task_id="ingest_sources", **task_options("source_ingestion"))()
    load = load_raw_task.override(task_id="load_raw", **task_options("source_ingestion"))()
    reconcile = reconcile_sources_task.override(
        task_id="reconcile_hard_deletes", **task_options("source_ingestion")
    )()
    ingest >> load >> reconcile
