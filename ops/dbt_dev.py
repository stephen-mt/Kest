#!/usr/bin/env python3
"""Run dbt for one developer without sourcing a generated shell file."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from kest_ingestion.workflows import run_dbt_build

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--developer", required=True)
    args = parser.parse_args()
    credentials = ROOT / ".runtime" / "developers" / f"{args.developer}.env"
    if not credentials.is_file():
        raise SystemExit(f"Run `make dev-init DEVELOPER={args.developer}` first")
    values: dict[str, str] = {}
    for line in credentials.read_text().splitlines():
        if line and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    run_dbt_build(target="dev", env={**os.environ, **values})


if __name__ == "__main__":
    main()
