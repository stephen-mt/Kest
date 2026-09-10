"""TaskFlow DAG for the CyberMarket batch publication barrier."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

DEFAULTS = {
    "owner": "cybermarket-data",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


@task.external_python(python="/opt/kest/venv/bin/python")
def publish_lakehouse() -> None:
    from workloads.cybermarket.batch.runner import run

    run()


@task.external_python(python="/opt/kest/venv/bin/python")
def validate_publication() -> None:
    from workloads.cybermarket.validation.state import verify

    verify("batch")


@dag(
    dag_id="cybermarket_batch",
    description="Publish stable CyberMarket Silver and Gold Iceberg snapshots",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["kest", "cybermarket", "batch"],
)
def cybermarket_batch():
    publish_lakehouse() >> validate_publication()


cybermarket_batch()
