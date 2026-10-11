from __future__ import annotations

from datetime import datetime

from _common import DBT_EXECUTABLE, DEFAULT_ARGS, cli_task, failure_callback
from airflow.sdk import DAG
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig

with DAG(
    dag_id="ecommerce_end_to_end",
    start_date=datetime(2026, 1, 1),
    # Source schedules are an explicit operator decision. The bundled workload
    # is a demo and must never start writing merely because the platform starts.
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["kest", "production"],
) as dag:
    ingest = cli_task("ingest_sources", "ingest", "source_ingestion")
    load = cli_task("load_raw", "load", "source_ingestion")
    reconcile = cli_task("reconcile_hard_deletes", "reconcile", "source_ingestion")
    transform = DbtTaskGroup(
        group_id="dbt_build_candidate",
        project_config=ProjectConfig("/opt/kest/transformations"),
        profile_config=ProfileConfig(
            profile_name="kest",
            target_name="production",
            profiles_yml_filepath="/opt/kest/transformations/profiles.yml",
        ),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
        operator_args={
            "pool": "clickhouse_transform",
            "install_deps": False,
            "on_failure_callback": failure_callback,
        },
    )
    publish = cli_task("publish_gold", "publish", "gold_publish")
    smoke = cli_task("business_smoke", "smoke", "clickhouse_transform")
    ingest >> load >> reconcile >> transform >> publish >> smoke
