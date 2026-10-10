from datetime import datetime, timedelta

from _common import DEFAULT_ARGS, failure_callback
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="backup_restore_verify",
    start_date=datetime(2026, 1, 1),
    schedule="0 3 * * 0",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["kest", "recovery"],
) as dag:
    backup = BashOperator(
        task_id="backup",
        bash_command="/opt/kest/ops/backup.sh",
        pool="heavy_backfill",
        execution_timeout=timedelta(hours=2),
        on_failure_callback=failure_callback,
        do_xcom_push=False,
    )
    verify = BashOperator(
        task_id="restore_verify",
        bash_command="/opt/kest/ops/restore_test.sh",
        pool="heavy_backfill",
        execution_timeout=timedelta(hours=2),
        on_failure_callback=failure_callback,
        do_xcom_push=False,
    )
    backup >> verify
