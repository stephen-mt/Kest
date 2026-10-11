"""Small, shell-free helpers used by host-side platform commands."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import BinaryIO

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env"
COMPOSE = ["docker", "compose", "--env-file", str(ENV_FILE), "-f", str(ROOT / "compose.yaml")]


def read_env() -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in ENV_FILE.read_text().splitlines():
        if raw and not raw.startswith("#") and "=" in raw:
            key, value = raw.split("=", 1)
            values[key] = value
    return values


def compose(
    *args: str,
    capture_output: bool = False,
    stdin: BinaryIO | None = None,
    stdout: BinaryIO | None = None,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        [*COMPOSE, *args],
        check=True,
        cwd=ROOT,
        env={**os.environ, **read_env()},
        capture_output=capture_output,
        stdin=stdin,
        stdout=stdout,
    )


def container_path(host_path: Path) -> str:
    return f"/opt/kest/{host_path.resolve().relative_to(ROOT)}"
