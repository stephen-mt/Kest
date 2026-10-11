from __future__ import annotations

from datetime import datetime

from _common import DEFAULT_ARGS, run_backfill_task, task_options
from airflow.sdk import DAG, Param

with DAG(
    dag_id="controlled_backfill",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    render_template_as_native_obj=True,
    params={
        "source": Param("postgres", enum=["postgres", "campaigns", "campaign_spend", "product_catalog"]),
        "start": Param("2026-01-01", type="string", format="date"),
        "end": Param("2026-01-01", type="string", format="date"),
        "reason": Param("operator-request", type="string", minLength=3),
        "dry_run": Param(True, type="boolean"),
    },
    tags=["kest", "backfill"],
) as dag:
    run_backfill_task.override(task_id="run_backfill", **task_options("heavy_backfill", 120))(
        source="{{ params.source }}",
        start="{{ params.start }}",
        end="{{ params.end }}",
        reason="{{ params.reason }}",
        dry_run="{{ params.dry_run }}",
    )
