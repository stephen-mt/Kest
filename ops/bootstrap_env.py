#!/usr/bin/env python3
"""Create local-only secrets and generated SeaweedFS configuration."""

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


def main() -> None:
    RUNTIME.mkdir(mode=0o700, parents=True, exist_ok=True)
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
        "REDIS_PASSWORD": token(),
        "AIRFLOW_SECRET_KEY": token(48),
        "AIRFLOW_FERNET_KEY": "",
        "SUPERSET_SECRET_KEY": token(48),
        "SUPERSET_ADMIN_USER": "admin",
        "SUPERSET_ADMIN_PASSWORD": token(),
        "CLICKHOUSE_ADMIN_PASSWORD": token(),
        "CLICKHOUSE_PIPELINE_PASSWORD": token(),
        "CLICKHOUSE_PUBLISH_PASSWORD": token(),
        "CLICKHOUSE_DA_PASSWORD": token(),
        "CLICKHOUSE_METADATA_PASSWORD": token(),
        "SEAWEED_WRITER_ACCESS_KEY": f"kestwriter{token(12)}",
        "SEAWEED_WRITER_SECRET_KEY": token(40),
        "SEAWEED_READER_ACCESS_KEY": f"kestreader{token(12)}",
        "SEAWEED_READER_SECRET_KEY": token(40),
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

    seaweed_dir = RUNTIME / "seaweed"
    seaweed_dir.mkdir(mode=0o700, exist_ok=True)
    s3_config = {
        "identities": [
            {
                "name": "pipeline-writer",
                "credentials": [
                    {
                        "accessKey": values["SEAWEED_WRITER_ACCESS_KEY"],
                        "secretKey": values["SEAWEED_WRITER_SECRET_KEY"],
                    }
                ],
                "actions": ["Read:raw", "Write:raw", "List:raw", "Admin:raw"],
            },
            {
                "name": "raw-reader",
                "credentials": [
                    {
                        "accessKey": values["SEAWEED_READER_ACCESS_KEY"],
                        "secretKey": values["SEAWEED_READER_SECRET_KEY"],
                    }
                ],
                "actions": ["Read:raw", "List:raw"],
            },
        ]
    }
    config_file = seaweed_dir / "s3.json"
    config_file.write_text(json.dumps(s3_config, indent=2) + "\n")
    config_file.chmod(0o600)


if __name__ == "__main__":
    main()
