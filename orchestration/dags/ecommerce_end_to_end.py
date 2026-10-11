from __future__ import annotations

from datetime import datetime

from _common import (
    DEFAULT_ARGS,
    business_smoke_task,
    dbt_candidate_group,
    ingest_sources_task,
    load_raw_task,
    publish_gold_task,
    reconcile_sources_task,
    task_options,
)
from airflow.sdk import DAG

with DAG(
    dag_id="ecommerce_end_to_end",
    start_date=datetime(2026, 1, 1),
    # Schedules are enabled only after a real source owner approves its SLA.
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["kest", "production"],
) as dag:
    ingest = ingest_sources_task.override(task_id="ingest_sources", **task_options("source_ingestion"))()
    load = load_raw_task.override(task_id="load_raw", **task_options("source_ingestion"))()
    reconcile = reconcile_sources_task.override(
        task_id="reconcile_hard_deletes", **task_options("source_ingestion")
    )()
    transform = dbt_candidate_group()
    publish = publish_gold_task.override(task_id="publish_gold", **task_options("gold_publish"))()
    smoke = business_smoke_task.override(
        task_id="business_smoke", **task_options("clickhouse_transform")
    )()
    ingest >> load >> reconcile >> transform >> publish >> smoke
