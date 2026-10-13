#!/usr/bin/env python3
"""Create local-only secrets and generated S3 policies."""

from __future__ import annotations

import json
import secrets
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env"
RUNTIME = ROOT / ".runtime"


def token(length: int = 32) -> str:
    return secrets.token_urlsafe(length).replace("-", "A").replace("_", "B")[:length]


def parse_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text().splitlines():
        if raw and not raw.startswith("#") and "=" in raw:
            key, value = raw.split("=", 1)
            values[key] = value
    return values


def chmod_if_owned(path: Path, mode: int) -> None:
    try:
        path.chmod(mode)
    except PermissionError:
        # A prior container may own an artifact directory. `make up` repairs
        # shared runtime modes from a root tooling container after the build.
        pass


def mkdir_shared(path: Path) -> None:
    try:
        path.mkdir(mode=0o777, parents=True, exist_ok=True)
        chmod_if_owned(path, 0o777)
    except PermissionError:
        pass


def main() -> None:
    RUNTIME.mkdir(mode=0o711, parents=True, exist_ok=True)
    chmod_if_owned(RUNTIME, 0o711)
    # These paths contain artifacts, not secrets, and are shared by host UID,
    # root-run tooling and Airflow's unprivileged UID.
    for shared_name in ("batches", "dlt", "dbt", "backups"):
        mkdir_shared(RUNTIME / shared_name)
    for shared_path in (RUNTIME / "dbt" / "target", RUNTIME / "dbt" / "logs"):
        mkdir_shared(shared_path)
    values = parse_env(ENV_FILE)
    defaults = {
        "COMPOSE_PROJECT_NAME": "kest",
        "KEST_TIMEZONE": "Asia/Ho_Chi_Minh",
        "POSTGRES_ADMIN_PASSWORD": token(),
        "BUSINESS_SOURCE_PASSWORD": token(),
        "PLATFORM_CONTROL_PASSWORD": token(),
        "AIRFLOW_DB_PASSWORD": token(),
        "SUPERSET_DB_PASSWORD": token(),
        "OPENMETADATA_DB_PASSWORD": token(),
        "KEYCLOAK_DB_PASSWORD": token(),
        "KEYCLOAK_ADMIN_USER": "kest-admin",
        "KEYCLOAK_ADMIN_PASSWORD": token(),
        "KEYCLOAK_PLATFORM_ADMIN_PASSWORD": token(),
        "KEYCLOAK_DE_PASSWORD": token(),
        "KEYCLOAK_DA_PASSWORD": token(),
        "AIRFLOW_OIDC_CLIENT_SECRET": token(48),
        "SUPERSET_OIDC_CLIENT_SECRET": token(48),
        "OPENMETADATA_OIDC_CLIENT_SECRET": token(48),
        "OPENMETADATA_INGESTION_CLIENT_SECRET": token(48),
        "KEST_AUTH_MODE": "oidc",
        "REDIS_PASSWORD": token(),
        "AIRFLOW_SECRET_KEY": token(48),
        "AIRFLOW_FERNET_KEY": "",
        "AIRFLOW_ADMIN_USER": "admin",
        "AIRFLOW_ADMIN_PASSWORD": token(),
        "SUPERSET_SECRET_KEY": token(48),
        "SUPERSET_ADMIN_USER": "admin",
        "SUPERSET_ADMIN_PASSWORD": token(),
        "CLICKHOUSE_ADMIN_PASSWORD": token(),
        "CLICKHOUSE_PIPELINE_PASSWORD": token(),
        "CLICKHOUSE_PUBLISH_PASSWORD": token(),
        "CLICKHOUSE_DA_PASSWORD": token(),
        "CLICKHOUSE_METADATA_PASSWORD": token(),
        "S3_ADMIN_ACCESS_KEY": f"kestadmin{token(12)}",
        "S3_ADMIN_SECRET_KEY": token(40),
        "S3_WRITER_ACCESS_KEY": f"kestwriter{token(12)}",
        "S3_WRITER_SECRET_KEY": token(40),
        "S3_READER_ACCESS_KEY": f"kestreader{token(12)}",
        "S3_READER_SECRET_KEY": token(40),
        "S3_BACKUP_ACCESS_KEY": f"kestbackup{token(12)}",
        "S3_BACKUP_SECRET_KEY": token(40),
        "PLATFORM_ALERT_WEBHOOK_URL": "",
        "CLICKHOUSE_MEMORY_LIMIT": "4g",
        "AIRFLOW_WORKER_CONCURRENCY": "3",
        "SUPERSET_WORKERS": "2",
    }
    changed = False
    for key, value in defaults.items():
        if key not in values:
            values[key] = value
            changed = True

    if not values.get("AIRFLOW_FERNET_KEY"):
        try:
            from cryptography.fernet import Fernet

            values["AIRFLOW_FERNET_KEY"] = Fernet.generate_key().decode()
        except ImportError:
            # Bootstrap must work before project dependencies are installed.
            import base64

            values["AIRFLOW_FERNET_KEY"] = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()
        changed = True

    if changed or not ENV_FILE.exists():
        ENV_FILE.write_text("\n".join(f"{key}={value}" for key, value in sorted(values.items())) + "\n")
        ENV_FILE.chmod(0o600)

    policy_dir = RUNTIME / "object-store"
    policy_dir.mkdir(mode=0o700, exist_ok=True)
    chmod_if_owned(policy_dir, 0o700)
    policies = {
        "writer-policy.json": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetBucketLocation",
                        "s3:GetBucketVersioning",
                        "s3:ListBucket",
                        "s3:ListBucketMultipartUploads",
                    ],
                    "Resource": ["arn:aws:s3:::raw", "arn:aws:s3:::restore-verify"],
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:AbortMultipartUpload",
                        "s3:ListMultipartUploadParts",
                    ],
                    "Resource": ["arn:aws:s3:::raw/*", "arn:aws:s3:::restore-verify/*"],
                },
            ],
        },
        "reader-policy.json": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
                    "Resource": ["arn:aws:s3:::raw"],
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject"],
                    "Resource": ["arn:aws:s3:::raw/*"],
                },
            ],
        },
        "backup-policy.json": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetBucketLocation",
                        "s3:ListBucket",
                        "s3:ListBucketMultipartUploads",
                    ],
                    "Resource": ["arn:aws:s3:::platform-backups"],
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:AbortMultipartUpload",
                        "s3:ListMultipartUploadParts",
                    ],
                    "Resource": ["arn:aws:s3:::platform-backups/*"],
                },
            ],
        },
    }
    for filename, policy in policies.items():
        policy_file = policy_dir / filename
        policy_file.write_text(json.dumps(policy, indent=2) + "\n")
        policy_file.chmod(0o600)


if __name__ == "__main__":
    main()
