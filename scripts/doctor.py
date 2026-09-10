#!/usr/bin/env python3
"""Check that a checkout can start the local Kest stack."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env"
BASE_SETTINGS = (
    "POSTGRES_SOURCE_IMAGE",
    "POSTGRES_SOURCE_DB",
    "POSTGRES_SOURCE_USER",
    "POSTGRES_SOURCE_PASSWORD",
    "POSTGRES_AIRFLOW_IMAGE",
    "POSTGRES_AIRFLOW_DB",
    "POSTGRES_AIRFLOW_USER",
    "POSTGRES_AIRFLOW_PASSWORD",
    "POSTGRES_CATALOG_IMAGE",
    "POSTGRES_CATALOG_DB",
    "POSTGRES_CATALOG_USER",
    "POSTGRES_CATALOG_PASSWORD",
    "MINIO_IMAGE",
    "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD",
    "MINIO_BUCKET",
    "LAKEKEEPER_IMAGE",
    "LAKEKEEPER_PG_ENCRYPTION_KEY",
    "LAKEKEEPER_WAREHOUSE",
    "RISINGWAVE_IMAGE",
    "RISINGWAVE_TOTAL_MEMORY_BYTES",
    "RISINGWAVE_PARALLELISM",
    "AIRFLOW_IMAGE",
    "AIRFLOW_ADMIN_USER",
    "AIRFLOW_ADMIN_PASSWORD",
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_JWT_SECRET",
)


def parse_env(path: Path) -> dict[str, str]:
    values = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        values[name.strip()] = value.strip().strip("'\"")
    return values


def command_ok(command: list[str]) -> bool:
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


def status(ok: bool, message: str) -> None:
    print(f"[{'ok' if ok else 'error'}] {message}")


def main() -> int:
    failures = []

    env_exists = ENV_FILE.is_file()
    status(env_exists, ".env exists")
    if not env_exists:
        print("        cp .env.example .env, then replace every placeholder")
        return 1

    values = parse_env(ENV_FILE)
    missing = sorted(name for name in BASE_SETTINGS if not values.get(name))
    placeholders = sorted(
        name
        for name in BASE_SETTINGS
        if any(
            marker in values.get(name, "").lower()
            for marker in ("change_me", "replace_with")
        )
    )
    valid_env = not missing and not placeholders
    status(valid_env, "base configuration is complete")
    if missing:
        print(f"        missing: {', '.join(missing)}")
        failures.append("configuration")
    if placeholders:
        print(f"        placeholders: {', '.join(placeholders)}")
        failures.append("configuration")

    docker_cli = shutil.which("docker") is not None
    status(docker_cli, "Docker CLI is installed")
    if not docker_cli:
        failures.append("docker")
    else:
        compose_ok = command_ok(["docker", "compose", "version"])
        daemon_ok = command_ok(["docker", "info"])
        status(compose_ok, "Docker Compose v2 is available")
        status(daemon_ok, "Docker daemon is reachable")
        if not compose_ok or not daemon_ok:
            failures.append("docker")

    free_gib = shutil.disk_usage(ROOT).free / 1024**3
    enough_disk = free_gib >= 8
    label = f"{free_gib:.1f} GiB disk space is free"
    print(f"[{'ok' if enough_disk else 'warning'}] {label}")

    if failures:
        print("Kest is not ready. Fix the errors above and rerun `make doctor`.")
        return 1

    print("Kest is ready. Run `make demo` for the small first-run path.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
