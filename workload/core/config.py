import os
from dataclasses import dataclass


def required(name):
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
    event_rate: int
    random_seed: int
    cdc_slot: str
    cdc_batch_size: int
    cdc_poll_interval: float
    landing_prefix: str
    bronze_prefix: str
    bronze_target_bytes: int
    iceberg_prefix: str
    iceberg_catalog: str
    silver_namespace: str
    gold_namespace: str
    batch_duckdb_memory: str

    @classmethod
    def from_env(cls):
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
            event_rate=int(os.getenv("WORKLOAD_EVENT_RATE", "20")),
            random_seed=int(os.getenv("WORKLOAD_RANDOM_SEED", "20260909")),
            cdc_slot=os.getenv("CDC_SLOT_NAME", "kest_landing"),
            cdc_batch_size=int(os.getenv("CDC_BATCH_SIZE", "500")),
            cdc_poll_interval=float(os.getenv("CDC_POLL_INTERVAL_SECONDS", "1")),
            landing_prefix=os.getenv(
                "CDC_LANDING_PREFIX", "landing/postgres-source"
            ).strip("/"),
            bronze_prefix=os.getenv("BRONZE_PREFIX", "bronze/history").strip("/"),
            bronze_target_bytes=int(os.getenv("BRONZE_TARGET_BYTES", str(5 * 1024**3))),
            iceberg_prefix=os.getenv("ICEBERG_PREFIX", "iceberg").strip("/"),
            iceberg_catalog=os.getenv("ICEBERG_CATALOG", "kest"),
            silver_namespace=os.getenv("SILVER_NAMESPACE", "silver"),
            gold_namespace=os.getenv("GOLD_NAMESPACE", "gold"),
            batch_duckdb_memory=os.getenv("BATCH_DUCKDB_MEMORY", "1GB"),
        )

    def pg_kwargs(self):
        return {
            "host": self.pg_host,
            "port": self.pg_port,
            "dbname": self.pg_database,
            "user": self.pg_user,
            "password": self.pg_password,
        }
