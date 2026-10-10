from __future__ import annotations

from datetime import UTC, date, datetime

from _common import DEFAULT_ARGS, cli_task
from airflow.sdk import DAG, Param, task


@task(pool="heavy_backfill", retries=0)
def validate_window(**context) -> None:
    params = context["params"]
    start = date.fromisoformat(params["start"])
    end = date.fromisoformat(params["end"])
    if start > end:
        raise ValueError("start must not be after end")
    if end > datetime.now(UTC).date():
        raise ValueError("future backfills are not allowed")
    if (end - start).days > 366:
        raise ValueError("backfill window exceeds the 366-day local policy")
    if not params["reason"].strip():
        raise ValueError("reason is required")


with DAG(
    dag_id="controlled_backfill",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "source": Param("postgres", enum=["postgres", "campaigns", "campaign_spend", "product_catalog"]),
        "start": Param("2026-01-01", type="string", format="date"),
        "end": Param("2026-01-01", type="string", format="date"),
        "reason": Param("operator-request", type="string", minLength=3),
        "dry_run": Param(True, type="boolean"),
    },
    tags=["kest", "backfill"],
) as dag:
    checked = validate_window()
    run = cli_task(
        "run_backfill",
        "pipeline --backfill-source '{{ params.source }}' --start '{{ params.start }}' --end '{{ params.end }}' --reason '{{ params.reason }}' {{ '--dry-run' if params.dry_run else '' }}",
        "heavy_backfill",
        120,
    )
    checked >> run
