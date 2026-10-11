#!/usr/bin/env python3
"""Repair shared runtime directory ownership after containerized commands."""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / ".runtime"


def main() -> None:
    RUNTIME.mkdir(parents=True, exist_ok=True)
    RUNTIME.chmod(0o711)
    for relative in ("batches", "dlt", "dbt/logs", "dbt/target", "backups"):
        path = RUNTIME / relative
        path.mkdir(parents=True, exist_ok=True)
        for parent in (path, *path.parents):
            if parent == ROOT:
                break
            if parent.is_relative_to(RUNTIME):
                os.chmod(parent, 0o777)


if __name__ == "__main__":
    main()
