from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    postgres_host: str = os.getenv("POSTGRES_HOST", "postgres")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    business_password: str = os.getenv("BUSINESS_SOURCE_PASSWORD", "")
    control_password: str = os.getenv("PLATFORM_CONTROL_PASSWORD", "")
    clickhouse_host: str = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    clickhouse_port: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    clickhouse_admin_password: str = os.getenv("CLICKHOUSE_ADMIN_PASSWORD", "")
    clickhouse_pipeline_password: str = os.getenv("CLICKHOUSE_PIPELINE_PASSWORD", "")
    clickhouse_publish_password: str = os.getenv("CLICKHOUSE_PUBLISH_PASSWORD", "")
    clickhouse_da_password: str = os.getenv("CLICKHOUSE_DA_PASSWORD", "")
    s3_endpoint: str = os.getenv("S3_ENDPOINT", "http://object-store:9000")
    s3_writer_key: str = os.getenv("S3_WRITER_ACCESS_KEY", "")
    s3_writer_secret: str = os.getenv("S3_WRITER_SECRET_KEY", "")
    s3_reader_key: str = os.getenv("S3_READER_ACCESS_KEY", "")
    s3_reader_secret: str = os.getenv("S3_READER_SECRET_KEY", "")
    source_api_url: str = os.getenv("SOURCE_API_URL", "http://source-api:8000")
    source_config_dir: Path = Path(
        os.getenv("SOURCE_CONFIG_DIR", str(ROOT / "ingestion" / "config"))
    )
    runtime_dir: Path = Path(os.getenv("KEST_RUNTIME_DIR", str(ROOT / ".runtime")))

    @property
    def business_dsn(self) -> str:
        return (
            f"postgresql://business_source:{self.business_password}@"
            f"{self.postgres_host}:{self.postgres_port}/business_source"
        )

    @property
    def control_dsn(self) -> str:
        return (
            f"postgresql://platform_control:{self.control_password}@"
            f"{self.postgres_host}:{self.postgres_port}/platform_control"
        )


def require_secrets(settings: Settings) -> None:
    missing = [
        name
        for name, value in {
            "BUSINESS_SOURCE_PASSWORD": settings.business_password,
            "PLATFORM_CONTROL_PASSWORD": settings.control_password,
            "CLICKHOUSE_PIPELINE_PASSWORD": settings.clickhouse_pipeline_password,
            "S3_WRITER_ACCESS_KEY": settings.s3_writer_key,
            "S3_WRITER_SECRET_KEY": settings.s3_writer_secret,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
