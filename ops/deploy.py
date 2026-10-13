#!/usr/bin/env python3
"""Promote an immutable Kest release to staging or production."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

from bootstrap_env import write_object_store_policies

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = {
    "AIRFLOW_IMAGE",
    "TOOLING_IMAGE",
    "SUPERSET_IMAGE",
    "KEST_GIT_SHA",
    "POSTGRES_ADMIN_PASSWORD",
    "PLATFORM_CONTROL_PASSWORD",
    "CLICKHOUSE_ADMIN_PASSWORD",
    "KEYCLOAK_DB_PASSWORD",
}


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        if line and not line.lstrip().startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("environment", choices=["staging", "production"])
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    env_file = args.env_file or ROOT / f".env.{args.environment}"
    if not env_file.is_file():
        raise SystemExit(f"Missing deployment secret file: {env_file}")
    values = read_env(env_file)
    missing = sorted(name for name in REQUIRED if not values.get(name))
    if missing:
        raise SystemExit(f"Missing deployment values: {', '.join(missing)}")
    if values.get("KEST_ENVIRONMENT") != args.environment:
        raise SystemExit("KEST_ENVIRONMENT must match the deployment target")
    if values["KEST_GIT_SHA"] in {"workspace", "unknown", ""}:
        raise SystemExit("Production-like deploys require an immutable Git SHA")
    for image_var in ("AIRFLOW_IMAGE", "TOOLING_IMAGE", "SUPERSET_IMAGE"):
        image = values[image_var]
        if ":" not in image and "@sha256:" not in image:
            raise SystemExit(f"{image_var} must use an immutable SHA tag or digest")

    compose = [
        "docker", "compose", "--env-file", str(env_file),
        "-f", str(ROOT / "compose.yaml"),
        "-f", str(ROOT / "compose.production.yaml"),
        "--profile", "tools",
    ]
    env = {**os.environ, **values}

    def run(*arguments: str) -> None:
        subprocess.run([*compose, *arguments], cwd=ROOT, env=env, check=True)

    rendered = subprocess.run(
        [*compose, "config", "--format", "json"],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    model = json.loads(rendered.stdout)
    allowed_runtime_target = "/opt/kest/.runtime/object-store"
    for service, definition in model["services"].items():
        for volume in definition.get("volumes", []):
            if (
                volume.get("type") == "bind"
                and str(volume.get("target", "")).startswith("/opt/kest")
                and volume.get("target") != allowed_runtime_target
            ):
                raise SystemExit(f"Mutable checkout mounted into production service {service}")
    write_object_store_policies()
    run("pull", "tooling", "airflow-init", "superset-init")
    run("run", "--rm", "--no-deps", "--user", "root", "tooling", "python", "ops/prepare_runtime.py")
    run("up", "-d", "--no-build", "--wait", "postgres", "redis", "object-store", "clickhouse")
    run("run", "--rm", "--no-deps", "object-store-init")
    run("run", "--rm", "--no-deps", "tooling", "python", "-m", "ops.migrate")
    run("up", "-d", "--no-build", "--wait", "keycloak")
    run("run", "--rm", "--no-deps", "airflow-init")
    run("run", "--rm", "--no-deps", "superset-init")
    run(
        "up", "-d", "--no-build", "--wait",
        "airflow-api-server", "airflow-scheduler", "airflow-dag-processor",
        "airflow-worker", "airflow-triggerer", "superset", "superset-worker",
    )
    run("run", "--rm", "--no-deps", "tooling", "python", "-m", "kest_ingestion.cli", "health")
    print(f"deployed {values['KEST_GIT_SHA']} to {args.environment}")


if __name__ == "__main__":
    main()
