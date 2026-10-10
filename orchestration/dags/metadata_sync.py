from datetime import datetime

from _common import DEFAULT_ARGS, failure_callback
from airflow.providers.standard.operators.bash import BashOperator
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
    sync = BashOperator(
        task_id="external_openmetadata_ingestion",
        bash_command="/opt/kest/ops/metadata_sync.sh",
        pool="catalog_sync",
        execution_timeout=DEFAULT_ARGS["execution_timeout"],
        retries=1,
        on_failure_callback=failure_callback,
        do_xcom_push=False,
    )
