"""Application workflows shared by the CLI and Airflow tasks.

This module is deliberately free of Airflow imports.  Airflow only schedules
these functions; the same behaviour can be exercised synchronously by the
operator CLI and by tests.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import subprocess
import sys
import uuid
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path

import yaml

from kest_ingestion.config import ROOT, Settings
from kest_ingestion.db import business_connection, clickhouse_client, control_connection
from kest_ingestion.ingest import ensure_raw_bucket, ingest_all, ingest_backfill, reconcile_hard_deletes
from kest_ingestion.loader import load_pending
from kest_ingestion.models import SourceConfig
from kest_ingestion.publish import MARTS, publish_all


def run_dbt_build(target: str = "production", env: dict[str, str] | None = None) -> None:
    """Run dbt in its immutable, dependency-isolated virtual environment."""
    executable = Path("/opt/venvs/dbt/bin/dbt")
    if not executable.exists():
        executable = Path(sys.executable).parent / "dbt"
    subprocess.run(
        [
            str(executable),
            "build",
            "--project-dir",
            str(ROOT / "transformations"),
            "--profiles-dir",
            str(ROOT / "transformations"),
            "--target",
            target,
        ],
        check=True,
        env=env,
    )


def _run_identity() -> tuple[str, str | None, str | None]:
    dag_id = os.getenv("AIRFLOW_CTX_DAG_ID")
    orchestrator_run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID")
    if dag_id and orchestrator_run_id:
        run_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"kest:{os.getenv('KEST_ENVIRONMENT', 'local')}:{dag_id}:{orchestrator_run_id}",
            )
        )
        return run_id, "airflow", f"{dag_id}:{orchestrator_run_id}"
    return str(uuid.uuid4()), None, None


def start_run(
    settings: Settings,
    *,
    run_type: str = "PIPELINE",
    requested_by: str | None = None,
    reason: str | None = None,
    source: str | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
    metadata: dict[str, object] | None = None,
) -> str:
    run_id, orchestrator, orchestrator_run_id = _run_identity()
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_run(
              run_id, status, run_type, environment, git_sha, image_ref,
              orchestrator, orchestrator_run_id, requested_by, reason, source,
              window_start, window_end, metadata
            ) VALUES (%s,'RUNNING',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (run_id) DO UPDATE
              SET status='RUNNING', finished_at=NULL, error=NULL
            """,
            (
                run_id,
                run_type,
                os.getenv("KEST_ENVIRONMENT", "local"),
                os.getenv("KEST_GIT_SHA"),
                os.getenv("KEST_IMAGE_REF"),
                orchestrator,
                orchestrator_run_id,
                requested_by or os.getenv("KEST_REQUESTED_BY", "system"),
                reason,
                source,
                window_start,
                window_end,
                json.dumps(metadata or {}),
            ),
        )
    return run_id


def finish_run(settings: Settings, run_id: str, status: str, error: str | None = None) -> None:
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline_run SET status=%s, error=%s, finished_at=now() WHERE run_id=%s",
            (status, error, run_id),
        )


@contextmanager
def run_step(settings: Settings, run_id: str, step: str):
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT coalesce(max(attempt), 0) + 1 FROM pipeline_run_step WHERE run_id=%s AND step=%s",
            (run_id, step),
        )
        attempt = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO pipeline_run_step(run_id,step,status,attempt) VALUES (%s,%s,'RUNNING',%s)",
            (run_id, step, attempt),
        )
    try:
        yield
    except Exception as error:
        with control_connection(settings) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE pipeline_run_step SET status='FAILED', error=%s, finished_at=now() "
                "WHERE run_id=%s AND step=%s AND attempt=%s",
                (str(error)[:4000], run_id, step, attempt),
            )
        finish_run(settings, run_id, "FAILED", str(error)[:4000])
        raise
    else:
        with control_connection(settings) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE pipeline_run_step SET status='SUCCESS', finished_at=now() "
                "WHERE run_id=%s AND step=%s AND attempt=%s",
                (run_id, step, attempt),
            )


def attach_batches(settings: Settings, run_id: str, batch_ids: list[str]) -> None:
    if not batch_ids:
        return
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO pipeline_run_batch(run_id,batch_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
            [(run_id, batch_id) for batch_id in batch_ids],
        )


def fail_orchestrated_run(settings: Settings, dag_id: str, orchestrator_run_id: str, error: str) -> None:
    run_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"kest:{os.getenv('KEST_ENVIRONMENT', 'local')}:{dag_id}:{orchestrator_run_id}",
        )
    )
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline_run SET status='FAILED', error=%s, finished_at=now() WHERE run_id=%s",
            (error[:4000], run_id),
        )


def ingest_sources(settings: Settings) -> list[str]:
    return [batch.batch_id for batch in ingest_all(settings)]


def load_raw(settings: Settings) -> list[str]:
    return load_pending(settings)


def reconcile_sources(settings: Settings) -> str | None:
    result = reconcile_hard_deletes(settings)
    if result:
        load_pending(settings)
        return result.batch_id
    return None


def publish_gold(settings: Settings) -> dict[str, str]:
    run_id = start_run(settings, run_type="PUBLISH")
    try:
        with run_step(settings, run_id, "publish"):
            publications = publish_all(settings, run_id)
        finish_run(settings, run_id, "SUCCESS")
        return publications
    except Exception as error:
        finish_run(settings, run_id, "FAILED", str(error)[:4000])
        raise


def run_pipeline(settings: Settings) -> dict[str, object]:
    run_id = start_run(settings)
    try:
        with run_step(settings, run_id, "ingest"):
            batches = ingest_sources(settings)
            attach_batches(settings, run_id, batches)
        with run_step(settings, run_id, "load"):
            loaded = load_raw(settings)
        with run_step(settings, run_id, "transform"):
            run_dbt_build()
        with run_step(settings, run_id, "publish"):
            publications = publish_all(settings, run_id)
        finish_run(settings, run_id, "SUCCESS")
        return {
            "run_id": run_id,
            "batches": batches,
            "loaded": loaded,
            "publications": publications,
        }
    except Exception as error:
        finish_run(settings, run_id, "FAILED", str(error)[:4000])
        raise


def run_backfill(
    settings: Settings,
    source: str,
    start_value: str,
    end_value: str,
    reason: str,
) -> dict[str, object]:
    plan = validate_backfill(source, start_value, end_value, reason)
    run_id = start_run(
        settings,
        run_type="BACKFILL",
        reason=reason,
        source=source,
        window_start=start_value,
        window_end=end_value,
    )
    request_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"kest:backfill:{run_id}"))
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO backfill_request(
              request_id,run_id,source,window_start,window_end,reason,requested_by,status
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,'RUNNING')
            ON CONFLICT (request_id) DO UPDATE SET status='RUNNING', error=NULL, finished_at=NULL
            """,
            (request_id, run_id, source, start_value, end_value, reason, os.getenv("KEST_REQUESTED_BY", "system")),
        )
    try:
        with run_step(settings, run_id, "backfill_extract"):
            batches = ingest_backfill(
                settings,
                source,
                date.fromisoformat(start_value),
                date.fromisoformat(end_value),
            )
        batch_ids = [batch.batch_id for batch in batches]
        attach_batches(settings, run_id, batch_ids)
        with run_step(settings, run_id, "backfill_load"):
            loaded = load_pending(settings, batch_ids)
        with run_step(settings, run_id, "transform"):
            run_dbt_build()
        with run_step(settings, run_id, "publish"):
            publications = publish_all(settings, run_id)
        finish_run(settings, run_id, "SUCCESS")
        with control_connection(settings) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE backfill_request SET status='SUCCESS', finished_at=now() WHERE request_id=%s",
                (request_id,),
            )
        return {
            "run_id": run_id,
            "backfill": plan,
            "batches": batch_ids,
            "loaded": loaded,
            "publications": publications,
        }
    except Exception as error:
        finish_run(settings, run_id, "FAILED", str(error)[:4000])
        with control_connection(settings) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE backfill_request SET status='FAILED', error=%s, finished_at=now() WHERE request_id=%s",
                (str(error)[:4000], request_id),
            )
        raise


def health(settings: Settings) -> dict[str, str]:
    checks: dict[str, str] = {}
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")
        checks["platform_control"] = "ok" if cur.fetchone()[0] == 1 else "failed"
    client = clickhouse_client(settings, "pipeline")
    checks["clickhouse"] = "ok" if client.query("SELECT 1").first_row[0] == 1 else "failed"
    ensure_raw_bucket(settings)
    checks["object_store"] = "ok"
    return checks


def smoke(settings: Settings) -> dict[str, object]:
    client = clickhouse_client(settings, "da")
    counts = {mart: client.query(f"SELECT count() FROM gold.{mart}").first_row[0] for mart in MARTS}
    if any(count == 0 for count in counts.values()):
        raise RuntimeError(f"Empty Gold mart: {counts}")
    net = client.query("SELECT sum(net_revenue) FROM gold.mart_daily_revenue").first_row[0]
    with business_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              (SELECT coalesce(sum(amount), 0) FROM payments WHERE status='succeeded' AND NOT is_deleted)
              - (SELECT coalesce(sum(amount), 0) FROM refunds WHERE status='succeeded' AND NOT is_deleted)
            """
        )
        expected = cur.fetchone()[0]
    if net != expected:
        raise RuntimeError(f"Gold/source net revenue mismatch: gold={net}, source={expected}")
    return {"gold_rows": counts, "net_revenue": str(net), "source_expected": str(expected)}


def dev_init(settings: Settings, developer: str) -> dict[str, str]:
    if not re.fullmatch(r"[a-z][a-z0-9_]{1,31}", developer):
        raise ValueError("Developer must match [a-z][a-z0-9_]{1,31}")
    client = clickhouse_client(settings, "default")
    database = f"dev_{developer}"
    user = f"de_{developer}"
    credentials_dir = settings.runtime_dir / "developers"
    credentials_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    credentials_file = credentials_dir / f"{developer}.env"
    password = ""
    if credentials_file.exists():
        for line in credentials_file.read_text().splitlines():
            if line.startswith("DBT_DEV_PASSWORD="):
                password = line.split("=", 1)[1]
                break
    if not password:
        password = secrets.token_urlsafe(32)
    client.command(f"CREATE DATABASE IF NOT EXISTS {database} ENGINE=Atomic")
    client.command(
        f"CREATE USER IF NOT EXISTS {user} IDENTIFIED WITH sha256_password BY %(password)s",
        parameters={"password": password},
    )
    client.command(
        f"ALTER USER {user} IDENTIFIED WITH sha256_password BY %(password)s SETTINGS PROFILE development_limits",
        parameters={"password": password},
    )
    client.command(f"GRANT de_development TO {user}")
    client.command(f"GRANT ALL ON {database}.* TO {user}")
    credentials_file.write_text(f"DEVELOPER={developer}\nDBT_DEV_PASSWORD={password}\n")
    credentials_file.chmod(0o600)
    return {"database": database, "user": user, "credentials_file": str(credentials_file)}


def validate_configs(settings: Settings) -> None:
    for path in sorted(settings.source_config_dir.glob("*.yml")):
        SourceConfig.model_validate(yaml.safe_load(path.read_text()))


def validate_backfill(
    source: str,
    start_value: str | None,
    end_value: str | None,
    reason: str | None,
) -> dict[str, object]:
    configured = {
        SourceConfig.model_validate(yaml.safe_load(path.read_text())).name
        for path in Settings().source_config_dir.glob("*.yml")
    }
    allowed = {"postgres", "product_catalog", *configured}
    if source not in allowed:
        raise ValueError(f"source must be one of: {', '.join(sorted(allowed))}")
    if not start_value or not end_value:
        raise ValueError("start and end are required for a backfill")
    start = date.fromisoformat(start_value)
    end = date.fromisoformat(end_value)
    if start > end:
        raise ValueError("start must not be after end")
    if end > datetime.now(UTC).date():
        raise ValueError("future backfills are not allowed")
    if (end - start).days > 366:
        raise ValueError("backfill window exceeds the 366-day local policy")
    if not reason or len(reason.strip()) < 3:
        raise ValueError("reason must contain at least three characters")
    return {"source": source, "start": start_value, "end": end_value, "reason": reason}


def emit(payload: object) -> None:
    print(json.dumps(payload, indent=2))
