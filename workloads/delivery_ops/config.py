"""Runtime configuration for the DeliveryOps scenario."""

from __future__ import annotations

import os
from dataclasses import dataclass


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


@dataclass(frozen=True)
class Settings:
    pg_host: str
    pg_port: int
    pg_database: str
    pg_user: str
    pg_password: str
    s3_endpoint: str
    s3_bucket: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str
    iceberg_catalog: str
    iceberg_prefix: str
    random_seed: int
    shipment_count: int
    batch_memory: str
    risingwave_host: str
    risingwave_port: int
    risingwave_database: str
    risingwave_user: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            pg_host=required("PGHOST"),
            pg_port=int(os.getenv("PGPORT", "5432")),
            pg_database=required("PGDATABASE"),
            pg_user=required("PGUSER"),
            pg_password=required("PGPASSWORD"),
            s3_endpoint=required("S3_ENDPOINT_URL"),
            s3_bucket=required("S3_BUCKET"),
            aws_access_key_id=required("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=required("AWS_SECRET_ACCESS_KEY"),
            aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            iceberg_catalog=os.getenv("ICEBERG_CATALOG", "kest"),
            iceberg_prefix=os.getenv(
                "DELIVERY_ICEBERG_PREFIX", "iceberg/delivery_ops"
            ).strip("/"),
            random_seed=int(os.getenv("DELIVERY_RANDOM_SEED", "20260910")),
            shipment_count=int(os.getenv("DELIVERY_SHIPMENT_COUNT", "25000")),
            batch_memory=os.getenv("DELIVERY_BATCH_MEMORY", "1GB"),
            risingwave_host=os.getenv("RISINGWAVE_HOST", "risingwave"),
            risingwave_port=int(os.getenv("RISINGWAVE_PORT", "4566")),
            risingwave_database=os.getenv("RISINGWAVE_DATABASE", "dev"),
            risingwave_user=os.getenv("RISINGWAVE_USER", "root"),
        )

    def pg_kwargs(self) -> dict[str, object]:
        return {
            "host": self.pg_host,
            "port": self.pg_port,
            "dbname": self.pg_database,
            "user": self.pg_user,
            "password": self.pg_password,
        }

    def risingwave_kwargs(self) -> dict[str, object]:
        return {
            "host": self.risingwave_host,
            "port": self.risingwave_port,
            "dbname": self.risingwave_database,
            "user": self.risingwave_user,
        }
