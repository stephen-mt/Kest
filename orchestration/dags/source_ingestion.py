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
    cli_task("ingest_sources", "ingest", "source_ingestion") >> cli_task(
        "load_raw", "load", "source_ingestion"
    )
