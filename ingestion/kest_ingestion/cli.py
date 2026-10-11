from __future__ import annotations

import argparse
import json
import re
import secrets
import subprocess
import sys
import uuid
from datetime import UTC, date, datetime
from pathlib import Path

import yaml

from kest_ingestion.config import ROOT, Settings, require_secrets
from kest_ingestion.db import business_connection, clickhouse_client, control_connection
from kest_ingestion.ingest import ensure_raw_bucket, ingest_all, reconcile_hard_deletes
from kest_ingestion.loader import load_pending
from kest_ingestion.models import SourceConfig
from kest_ingestion.publish import MARTS, publish_all
from kest_ingestion.seed import add_late_refund, seed_business_data


def _dbt_build(target: str = "production") -> None:
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
    )


def _start_run(settings: Settings) -> str:
    run_id = str(uuid.uuid4())
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO pipeline_run(run_id, status) VALUES (%s, 'RUNNING')", (run_id,))
    return run_id


def _finish_run(settings: Settings, run_id: str, status: str, error: str | None = None) -> None:
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline_run SET status=%s, error=%s, finished_at=now() WHERE run_id=%s",
            (status, error, run_id),
        )


def run_pipeline(settings: Settings) -> None:
    run_id = _start_run(settings)
    try:
        batches = ingest_all(settings)
        loaded = load_pending(settings)
        _dbt_build()
        publications = publish_all(settings, run_id)
        _finish_run(settings, run_id, "SUCCESS")
        print(
            json.dumps(
                {
                    "run_id": run_id,
                    "batches": [b.batch_id for b in batches],
                    "loaded": loaded,
                    "publications": publications,
                },
                indent=2,
            )
        )
    except Exception as error:
        _finish_run(settings, run_id, "FAILED", str(error)[:4000])
        raise


def health(settings: Settings) -> None:
    checks: dict[str, str] = {}
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")
        checks["platform_control"] = "ok" if cur.fetchone()[0] == 1 else "failed"
    client = clickhouse_client(settings, "pipeline")
    checks["clickhouse"] = "ok" if client.query("SELECT 1").first_row[0] == 1 else "failed"
    ensure_raw_bucket(settings)
    checks["object_store"] = "ok"
    print(json.dumps(checks, indent=2))


def smoke(settings: Settings) -> None:
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
    print(
        json.dumps({"gold_rows": counts, "net_revenue": str(net), "source_expected": str(expected)}, indent=2)
    )


def dev_init(settings: Settings, developer: str) -> None:
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
    print(json.dumps({"database": database, "user": user, "credentials_file": str(credentials_file)}))


def validate_configs(settings: Settings) -> None:
    for path in sorted(settings.source_config_dir.glob("*.yml")):
        SourceConfig.model_validate(yaml.safe_load(path.read_text()))
    print("source configuration: valid")


def _validate_backfill(source: str, start_value: str | None, end_value: str | None, reason: str | None) -> None:
    allowed = {"postgres", "campaigns", "campaign_spend", "product_catalog"}
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


def main() -> None:
    parser = argparse.ArgumentParser(prog="kest")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("seed")
    commands.add_parser("ingest")
    commands.add_parser("load")
    commands.add_parser("transform")
    commands.add_parser("publish")
    pipeline_parser = commands.add_parser("pipeline")
    pipeline_parser.add_argument("--backfill-source")
    pipeline_parser.add_argument("--start")
    pipeline_parser.add_argument("--end")
    pipeline_parser.add_argument("--reason")
    pipeline_parser.add_argument("--dry-run", action="store_true")
    commands.add_parser("health")
    commands.add_parser("smoke")
    commands.add_parser("late-refund")
    commands.add_parser("reconcile")
    commands.add_parser("validate-config")
    dev = commands.add_parser("dev-init")
    dev.add_argument("--developer", required=True)
    args = parser.parse_args()
    settings = Settings()
    require_secrets(settings)

    if args.command == "seed":
        seed_business_data(settings)
    elif args.command == "ingest":
        print(json.dumps([result.model_dump(mode="json") for result in ingest_all(settings)], indent=2))
    elif args.command == "load":
        print(json.dumps(load_pending(settings)))
    elif args.command == "transform":
        _dbt_build()
    elif args.command == "publish":
        run_id = _start_run(settings)
        try:
            print(json.dumps(publish_all(settings, run_id), indent=2))
            _finish_run(settings, run_id, "SUCCESS")
        except Exception as error:
            _finish_run(settings, run_id, "FAILED", str(error))
            raise
    elif args.command == "pipeline":
        if args.backfill_source:
            _validate_backfill(args.backfill_source, args.start, args.end, args.reason)
        if args.backfill_source and args.dry_run:
            print(
                json.dumps(
                    {
                        "dry_run": True,
                        "source": args.backfill_source,
                        "start": args.start,
                        "end": args.end,
                        "reason": args.reason,
                    },
                    indent=2,
                )
            )
        else:
            run_pipeline(settings)
    elif args.command == "health":
        health(settings)
    elif args.command == "smoke":
        smoke(settings)
    elif args.command == "late-refund":
        add_late_refund(settings)
    elif args.command == "reconcile":
        result = reconcile_hard_deletes(settings)
        loaded = load_pending(settings) if result else []
        print(json.dumps({"batch": result.batch_id if result else None, "loaded": loaded}, indent=2))
    elif args.command == "dev-init":
        dev_init(settings, args.developer)
    elif args.command == "validate-config":
        validate_configs(settings)


if __name__ == "__main__":
    main()
