from datetime import datetime

from _common import DEFAULT_ARGS, catalog_sync_task, failure_callback
from airflow.sdk import DAG

with DAG(
    dag_id="metadata_sync",
    start_date=datetime(2026, 1, 1),
    schedule="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["kest", "catalog"],
) as dag:
    catalog_sync_task.override(task_id="sync_openmetadata", on_failure_callback=failure_callback)()
