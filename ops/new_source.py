#!/usr/bin/env python3
"""Create a validated REST source configuration without pipeline glue code."""

from __future__ import annotations

import argparse
import re

import yaml
from kest_ingestion.config import ROOT
from kest_ingestion.models import SourceConfig


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--table")
    parser.add_argument("--primary-key", default="id")
    parser.add_argument("--updated-at", default="updated_at")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z][a-z0-9_]{1,62}", args.name):
        raise ValueError("SOURCE must match [a-z][a-z0-9_]{1,62}")
    table = args.table or args.name
    raw = {
        "name": args.name,
        "base_url": args.base_url.rstrip("/"),
        "endpoint": "/" + args.endpoint.lstrip("/"),
        "table_name": table,
        "primary_key": args.primary_key,
        "updated_at": args.updated_at,
        "data_path": "items",
        "cursor_param": "cursor",
        "next_cursor_path": "next_cursor",
        "page_size": 100,
        "timeout_seconds": 10,
        "max_retries": 4,
    }
    config = SourceConfig.model_validate(raw)
    destination = ROOT / "ingestion" / "config" / f"{args.name}.yml"
    if destination.exists():
        raise FileExistsError(f"Source already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(yaml.safe_dump(config.model_dump(), sort_keys=False))
    print(f"Created {destination}")
    print("Next: review the pagination fields, then run `make pipeline`.")
    print(f"New records land in raw.source_records with table_name='{table}'.")


if __name__ == "__main__":
    main()
