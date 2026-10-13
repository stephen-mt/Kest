from __future__ import annotations

import argparse

from kest_ingestion.config import Settings, require_secrets
from kest_ingestion.ingest import ingest_all
from kest_ingestion.loader import load_pending
from kest_ingestion.publish import rollback_mart
from kest_ingestion.seed import add_late_refund, seed_business_data
from kest_ingestion.workflows import (
    dev_init,
    emit,
    health,
    publish_gold,
    reconcile_sources,
    run_backfill,
    run_dbt_build,
    run_pipeline,
    smoke,
    validate_backfill,
    validate_configs,
)


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
    failed = commands.add_parser("fail-run")
    failed.add_argument("--dag-id", required=True)
    failed.add_argument("--orchestrator-run-id", required=True)
    failed.add_argument("--error", required=True)
    rollback = commands.add_parser("rollback")
    rollback.add_argument("--dataset", required=True)
    rollback.add_argument("--publication-id", required=True)
    rollback.add_argument("--reason", required=True)
    rollback.add_argument("--execute", action="store_true")
    dev = commands.add_parser("dev-init")
    dev.add_argument("--developer", required=True)
    args = parser.parse_args()
    settings = Settings()
    require_secrets(settings)

    if args.command == "seed":
        seed_business_data(settings)
    elif args.command == "ingest":
        emit([result.model_dump(mode="json") for result in ingest_all(settings)])
    elif args.command == "load":
        emit(load_pending(settings))
    elif args.command == "transform":
        run_dbt_build()
    elif args.command == "publish":
        emit(publish_gold(settings))
    elif args.command == "pipeline":
        if args.backfill_source:
            plan = validate_backfill(args.backfill_source, args.start, args.end, args.reason)
        if args.backfill_source and args.dry_run:
            emit({"dry_run": True, **plan})
        else:
            if args.backfill_source:
                emit(run_backfill(settings, args.backfill_source, args.start, args.end, args.reason))
            else:
                emit(run_pipeline(settings))
    elif args.command == "health":
        emit(health(settings))
    elif args.command == "smoke":
        emit(smoke(settings))
    elif args.command == "late-refund":
        add_late_refund(settings)
    elif args.command == "reconcile":
        emit({"batch": reconcile_sources(settings)})
    elif args.command == "dev-init":
        emit(dev_init(settings, args.developer))
    elif args.command == "validate-config":
        validate_configs(settings)
        print("source configuration: valid")
    elif args.command == "fail-run":
        from kest_ingestion.workflows import fail_orchestrated_run

        fail_orchestrated_run(settings, args.dag_id, args.orchestrator_run_id, args.error)
    elif args.command == "rollback":
        emit(
            rollback_mart(
                settings,
                args.dataset,
                args.publication_id,
                args.reason,
                dry_run=not args.execute,
            )
        )


if __name__ == "__main__":
    main()
