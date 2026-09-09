from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="cybermarket_batch",
    description="Publish versioned CyberMarket Silver and Gold through one pointer",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["kest", "cybermarket", "batch"],
) as dag:
    publish = BashOperator(
        task_id="publish_silver_and_gold",
        bash_command="/opt/kest/venv/bin/python -m workload.pipelines.batch",
        cwd="/opt/kest",
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
    )
    validate = BashOperator(
        task_id="validate_batch",
        bash_command=(
            "/opt/kest/venv/bin/python -m workload.validation.state --phase batch"
        ),
        cwd="/opt/kest",
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
    )

    publish >> validate
