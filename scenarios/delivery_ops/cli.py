"""Command line entry point for DeliveryOps lifecycle jobs."""

from __future__ import annotations

import argparse
import json

from scenarios.delivery_ops.batch import (
    GOLD_QUERIES,
    SILVER_QUERIES,
    build_gold_job,
    build_silver_job,
    prepare_run,
    publish_run,
    run_quality_job,
)
from scenarios.delivery_ops.batch import (
    run as run_batch,
)
from scenarios.delivery_ops.config import Settings
from scenarios.delivery_ops.source import (
    apply_schema,
    emit_live_changes,
    release_replication_slot,
    seed,
)
from scenarios.delivery_ops.streaming import check as check_streaming
from scenarios.delivery_ops.streaming import setup as setup_streaming
from scenarios.delivery_ops.streaming import teardown as teardown_streaming
from scenarios.delivery_ops.validate import check_source
from scenarios.delivery_ops.validate import run as run_validation


def main() -> None:
    parser = argparse.ArgumentParser(description="Operate the DeliveryOps scenario")
    commands = parser.add_subparsers(dest="command", required=True)
    setup = commands.add_parser("setup", help="Create the source schema")
    setup.add_argument("--reset", action="store_true")
    commands.add_parser("seed", help="Generate deterministic dirty source data")
    commands.add_parser(
        "source-check", help="Validate source contract and dirty fixtures"
    )
    commands.add_parser("batch", help="Publish versioned Bronze, Silver, and Gold")
    prepare = commands.add_parser("prepare", help="Extract one Bronze snapshot")
    prepare.add_argument("--batch-id", required=True)
    silver = commands.add_parser("silver", help="Run one Silver transform")
    silver.add_argument("--batch-id", required=True)
    silver.add_argument("--table", choices=sorted(SILVER_QUERIES), required=True)
    gold = commands.add_parser("gold", help="Run one Gold transform")
    gold.add_argument("--batch-id", required=True)
    gold.add_argument("--table", choices=sorted(GOLD_QUERIES), required=True)
    quality = commands.add_parser("quality", help="Run the batch quality gate")
    quality.add_argument("--batch-id", required=True)
    publish = commands.add_parser("publish", help="Atomically publish a staged batch")
    publish.add_argument("--batch-id", required=True)
    commands.add_parser("validate", help="Validate the current published batch")
    live = commands.add_parser("emit-live", help="Generate CDC-safe live changes")
    live.add_argument("--events", type=int, default=20)
    commands.add_parser("streaming-setup", help="Create RisingWave CDC views")
    commands.add_parser("streaming-check", help="Validate RisingWave CDC views")
    commands.add_parser("streaming-down", help="Drop RisingWave CDC objects and slot")
    commands.add_parser("cdc-down", help="Release the stopped NiFi CDC slot")
    args = parser.parse_args()
    settings = Settings.from_env()

    if args.command == "setup":
        apply_schema(settings, reset=args.reset)
    elif args.command == "seed":
        seed(settings)
    elif args.command == "source-check":
        print(json.dumps(check_source(settings), indent=2, sort_keys=True))
    elif args.command == "batch":
        run_batch(settings)
    elif args.command == "prepare":
        prepare_run(settings, args.batch_id)
    elif args.command == "silver":
        build_silver_job(settings, args.batch_id, args.table)
    elif args.command == "gold":
        build_gold_job(settings, args.batch_id, args.table)
    elif args.command == "quality":
        run_quality_job(settings, args.batch_id)
    elif args.command == "publish":
        publish_run(settings, args.batch_id)
    elif args.command == "validate":
        run_validation(settings)
    elif args.command == "emit-live":
        emit_live_changes(settings, event_count=args.events)
    elif args.command == "streaming-setup":
        setup_streaming(settings)
    elif args.command == "streaming-check":
        check_streaming(settings)
    elif args.command == "streaming-down":
        teardown_streaming(settings)
    elif args.command == "cdc-down":
        release_replication_slot(settings, "kest_delivery_nifi")


if __name__ == "__main__":
    main()
