#!/usr/bin/env python3
"""Promote an immutable Kest release to staging or production."""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import subprocess
import time
from pathlib import Path

from bootstrap_env import write_object_store_policies

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = {
    "AIRFLOW_IMAGE",
    "TOOLING_IMAGE",
    "SUPERSET_IMAGE",
    "KEST_GIT_SHA",
    "KEST_IMAGE_REF",
    "POSTGRES_ADMIN_PASSWORD",
    "BUSINESS_SOURCE_PASSWORD",
    "PLATFORM_CONTROL_PASSWORD",
    "AIRFLOW_DB_PASSWORD",
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_SECRET_KEY",
    "AIRFLOW_ADMIN_USER",
    "AIRFLOW_ADMIN_PASSWORD",
    "SUPERSET_DB_PASSWORD",
    "SUPERSET_SECRET_KEY",
    "SUPERSET_ADMIN_USER",
    "SUPERSET_ADMIN_PASSWORD",
    "OPENMETADATA_DB_PASSWORD",
    "CLICKHOUSE_ADMIN_PASSWORD",
    "CLICKHOUSE_PIPELINE_PASSWORD",
    "CLICKHOUSE_PUBLISH_PASSWORD",
    "CLICKHOUSE_DA_PASSWORD",
    "CLICKHOUSE_METADATA_PASSWORD",
    "REDIS_PASSWORD",
    "KEYCLOAK_DB_PASSWORD",
    "KEYCLOAK_ADMIN_USER",
    "KEYCLOAK_ADMIN_PASSWORD",
    "KEYCLOAK_PLATFORM_ADMIN_PASSWORD",
    "KEYCLOAK_DE_PASSWORD",
    "KEYCLOAK_DA_PASSWORD",
    "AIRFLOW_OIDC_CLIENT_SECRET",
    "SUPERSET_OIDC_CLIENT_SECRET",
    "OPENMETADATA_OIDC_CLIENT_SECRET",
    "OPENMETADATA_INGESTION_CLIENT_SECRET",
    "GRAFANA_OIDC_CLIENT_SECRET",
    "GRAFANA_ADMIN_PASSWORD",
    "S3_ADMIN_ACCESS_KEY",
    "S3_ADMIN_SECRET_KEY",
    "S3_WRITER_ACCESS_KEY",
    "S3_WRITER_SECRET_KEY",
    "S3_READER_ACCESS_KEY",
    "S3_READER_SECRET_KEY",
    "S3_BACKUP_ACCESS_KEY",
    "S3_BACKUP_SECRET_KEY",
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
    mode = stat.S_IMODE(env_file.stat().st_mode)
    if mode & 0o077:
        raise SystemExit(f"Deployment secret file must be mode 0600: {env_file} is {mode:04o}")
    values = read_env(env_file)
    missing = sorted(name for name in REQUIRED if not values.get(name))
    if missing:
        raise SystemExit(f"Missing deployment values: {', '.join(missing)}")
    if values.get("KEST_ENVIRONMENT") != args.environment:
        raise SystemExit("KEST_ENVIRONMENT must match the deployment target")
    if values.get("KEST_AUTH_MODE") != "oidc":
        raise SystemExit("Production-like deploys require KEST_AUTH_MODE=oidc")
    git_sha = values["KEST_GIT_SHA"]
    if not re.fullmatch(r"[0-9a-f]{40}([0-9a-f]{24})?", git_sha):
        raise SystemExit("Production-like deploys require a full 40- or 64-character Git SHA")
    if values["KEST_IMAGE_REF"] != git_sha:
        raise SystemExit("KEST_IMAGE_REF must equal KEST_GIT_SHA")
    for image_var in ("AIRFLOW_IMAGE", "TOOLING_IMAGE", "SUPERSET_IMAGE"):
        image = values[image_var]
        if "@sha256:" not in image and not image.endswith(f":{git_sha}"):
            raise SystemExit(f"{image_var} must use an immutable SHA tag or digest")

    env = {**os.environ, **values}
    env["KEST_SERVICE_ENV_FILE"] = str(env_file.resolve())
    env["OPENMETADATA_REQUIRED"] = "true"
    compose = [
        "docker", "compose", "--env-file", str(env_file),
        "-f", str(ROOT / "compose.yaml"),
        "-f", str(ROOT / "compose.production.yaml"),
        "--profile", "tools", "--profile", "observability", "--profile", "catalog",
    ]

    def run(*arguments: str) -> None:
        subprocess.run([*compose, *arguments], cwd=ROOT, env=env, check=True)

    def run_retry(*arguments: str, attempts: int = 6) -> None:
        for attempt in range(1, attempts + 1):
            try:
                run(*arguments)
                return
            except subprocess.CalledProcessError:
                if attempt == attempts:
                    raise
                time.sleep(5)

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
    run("run", "--rm", "--no-deps", "tooling", "python", "ops/keycloak_sync.py")
    run("run", "--rm", "--no-deps", "airflow-init")
    run("run", "--rm", "--no-deps", "superset-init")
    run(
        "up", "-d", "--no-build", "--wait",
        "airflow-api-server", "airflow-scheduler", "airflow-dag-processor",
        "airflow-worker", "airflow-triggerer", "superset", "superset-worker",
    )
    run("up", "-d", "--no-build", "--wait", "opensearch", "openmetadata-migrate", "openmetadata-server")
    run_retry("exec", "-T", "airflow-api-server", "airflow", "dags", "unpause", "metadata_sync")
    run(
        "up", "-d", "--no-build", "--wait",
        "platform-exporter", "postgres-exporter", "redis-exporter", "node-exporter",
        "prometheus", "alertmanager", "grafana",
    )
    run("run", "--rm", "--no-deps", "tooling", "python", "-m", "kest_ingestion.cli", "health")
    print(f"deployed {values['KEST_GIT_SHA']} to {args.environment}")


if __name__ == "__main__":
    main()
