from datetime import datetime

from _common import DEFAULT_ARGS, cli_task
from airflow.sdk import DAG

with DAG(
    dag_id="transform_publish",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["kest", "publish"],
) as dag:
    cli_task("dbt_build_candidate", "transform", "clickhouse_transform", 45) >> cli_task(
        "publish_gold", "publish", "gold_publish"
    )
