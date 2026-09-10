"""TaskFlow DAG for the complete DeliveryOps data product."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import TaskGroup, dag, get_current_context, task

SILVER_TABLES = (
    "dim_merchant_scd2",
    "dim_courier_scd2",
    "dim_customer",
    "dim_location",
    "fact_shipment",
    "fact_shipment_event",
    "fact_delivery_attempt",
    "fact_charge",
    "fact_route_stop",
    "shipment_current_state",
    "dq_rejected_records",
)
GOLD_TABLES = (
    "daily_delivery_sla",
    "hub_throughput",
    "courier_productivity",
    "merchant_delivery_scorecard",
    "delivery_failure_analysis",
    "route_efficiency",
    "delivery_margin_daily",
    "data_quality_daily",
)
DEFAULTS = {
    "owner": "delivery-data",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=45),
}


@task
def batch_identity() -> str:
    return get_current_context()["run_id"]


@task.external_python(python="/opt/kest/venv/bin/python")
def source_contract() -> None:
    from scenarios.delivery_ops.config import Settings
    from scenarios.delivery_ops.validate import check_source

    check_source(Settings.from_env())


@task.external_python(python="/opt/kest/venv/bin/python")
def bronze_snapshot(batch_id: str) -> None:
    from scenarios.delivery_ops.batch import prepare_run
    from scenarios.delivery_ops.config import Settings

    prepare_run(Settings.from_env(), batch_id)


@task.external_python(python="/opt/kest/venv/bin/python")
def silver_transform(name: str, batch_id: str) -> None:
    from scenarios.delivery_ops.batch import build_silver_job
    from scenarios.delivery_ops.config import Settings

    build_silver_job(Settings.from_env(), batch_id, name)


@task.external_python(python="/opt/kest/venv/bin/python")
def gold_transform(name: str, batch_id: str) -> None:
    from scenarios.delivery_ops.batch import build_gold_job
    from scenarios.delivery_ops.config import Settings

    build_gold_job(Settings.from_env(), batch_id, name)


@task.external_python(python="/opt/kest/venv/bin/python")
def quality_gate(batch_id: str) -> None:
    from scenarios.delivery_ops.batch import run_quality_job
    from scenarios.delivery_ops.config import Settings

    run_quality_job(Settings.from_env(), batch_id)


@task.external_python(python="/opt/kest/venv/bin/python")
def atomic_publish(batch_id: str) -> None:
    from scenarios.delivery_ops.batch import publish_run
    from scenarios.delivery_ops.config import Settings

    publish_run(Settings.from_env(), batch_id)


@dag(
    dag_id="delivery_ops_batch",
    description="PostgreSQL snapshot to governed Bronze, Silver, and Gold Iceberg",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args=DEFAULTS,
    tags=["delivery", "iceberg", "data-product", "governed"],
)
def delivery_ops_batch():
    batch_id = batch_identity()
    contract = source_contract()
    bronze = bronze_snapshot(batch_id)
    contract >> bronze

    with TaskGroup("silver", tooltip="Conformed facts, dimensions, and quarantine"):
        silver_jobs = {
            name: silver_transform.override(task_id=name)(name, batch_id)
            for name in SILVER_TABLES
        }
        silver_jobs["fact_shipment"] >> silver_jobs["shipment_current_state"]
        silver_jobs["fact_shipment_event"] >> silver_jobs["shipment_current_state"]

    with TaskGroup("gold", tooltip="Business metrics and operational marts"):
        gold_jobs = {
            name: gold_transform.override(task_id=name)(name, batch_id)
            for name in GOLD_TABLES
        }

    quality = quality_gate(batch_id)
    publish = atomic_publish(batch_id)
    for silver_job in silver_jobs.values():
        bronze >> silver_job >> quality
        for gold_job in gold_jobs.values():
            silver_job >> gold_job
    quality >> publish
    for gold_job in gold_jobs.values():
        gold_job >> publish


delivery_ops_batch()
