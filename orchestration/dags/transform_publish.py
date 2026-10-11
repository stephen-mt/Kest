from datetime import datetime

from _common import DEFAULT_ARGS, dbt_candidate_group, publish_gold_task, task_options
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
    dbt_candidate_group() >> publish_gold_task.override(
        task_id="publish_gold", **task_options("gold_publish")
    )()
