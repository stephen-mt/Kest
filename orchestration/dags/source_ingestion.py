from datetime import datetime

from _common import DEFAULT_ARGS, cli_task
from airflow.sdk import DAG

with DAG(
    dag_id="source_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["kest", "ingestion"],
) as dag:
    ingest = cli_task("ingest_sources", "ingest", "source_ingestion")
    load = cli_task("load_raw", "load", "source_ingestion")
    reconcile = cli_task("reconcile_hard_deletes", "reconcile", "source_ingestion")
    ingest >> load >> reconcile
