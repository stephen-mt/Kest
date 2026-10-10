from __future__ import annotations

import hashlib
import io
import json
from collections import defaultdict
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pyarrow.parquet as pq

from kest_ingestion.config import Settings
from kest_ingestion.db import clickhouse_client, control_connection
from kest_ingestion.ingest import _s3_client

RAW_COLUMNS: dict[str, list[str]] = {
    "customers": ["customer_id", "email", "country", "created_at", "updated_at", "is_deleted"],
    "orders": ["order_id", "customer_id", "status", "currency", "ordered_at", "updated_at", "is_deleted"],
    "order_items": [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "updated_at",
        "is_deleted",
    ],
    "payments": [
        "payment_id",
        "order_id",
        "provider_transaction_id",
        "status",
        "amount",
        "succeeded_at",
        "updated_at",
        "is_deleted",
    ],
    "refunds": ["refund_id", "payment_id", "status", "amount", "succeeded_at", "updated_at", "is_deleted"],
    "events": [
        "event_id",
        "event_type",
        "session_id",
        "customer_id",
        "product_id",
        "campaign_id",
        "occurred_at",
        "updated_at",
        "is_deleted",
    ],
    "marketing_campaigns": [
        "campaign_id",
        "name",
        "channel",
        "starts_at",
        "ends_at",
        "updated_at",
        "is_deleted",
    ],
    "campaign_spend": ["spend_id", "campaign_id", "spend_date", "amount", "updated_at", "is_deleted"],
    "product_catalog": ["product_id", "product_name", "category", "unit_cost", "updated_at", "is_deleted"],
}

DATE_COLUMNS = {"spend_date"}
DATETIME_COLUMNS = {
    "created_at",
    "updated_at",
    "ordered_at",
    "succeeded_at",
    "occurred_at",
    "starts_at",
    "ends_at",
}
DECIMAL_COLUMNS = {"unit_price", "amount", "unit_cost"}


def _coerce(column: str, value: Any) -> Any:
    if value is None:
        return None
    if column in DATE_COLUMNS and isinstance(value, str):
        return date.fromisoformat(value)
    if column in DATETIME_COLUMNS and isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    if column in DECIMAL_COLUMNS and not isinstance(value, Decimal):
        return Decimal(str(value))
    return value


def _record_hash(row: dict[str, Any], columns: list[str]) -> str:
    payload = json.dumps(
        {column: str(row.get(column)) if row.get(column) is not None else None for column in columns},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _download_rows(settings: Settings, key: str, expected_checksum: str) -> list[dict[str, Any]]:
    body = _s3_client(settings, writer=False).get_object(Bucket="raw", Key=key)["Body"].read()
    if hashlib.sha256(body).hexdigest() != expected_checksum:
        raise RuntimeError(f"Checksum mismatch for s3://raw/{key}")
    return pq.read_table(io.BytesIO(body)).to_pylist()


def _expected_objects(
    settings: Settings, batch_id: str
) -> tuple[str, str, dict[str, list[tuple[str, str, int]]]]:
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute("SELECT source, status FROM batch_manifest WHERE batch_id = %s", (batch_id,))
        manifest = cur.fetchone()
        if not manifest:
            raise ValueError(f"Unknown batch: {batch_id}")
        cur.execute(
            "SELECT table_name, object_path, checksum, row_count FROM batch_objects WHERE batch_id = %s ORDER BY object_path",
            (batch_id,),
        )
        grouped: dict[str, list[tuple[str, str, int]]] = defaultdict(list)
        for table, path, checksum, rows in cur.fetchall():
            grouped[table].append((path, checksum, rows))
    return manifest[0], manifest[1], grouped


def load_batch(settings: Settings, batch_id: str) -> bool:
    source, status, grouped = _expected_objects(settings, batch_id)
    if status == "LOADED":
        return False
    with control_connection(settings, autocommit=True) as lock_conn, lock_conn.cursor() as lock_cur:
        lock_cur.execute("SELECT pg_advisory_lock(hashtext(%s))", (f"load:{source}",))
        try:
            with control_connection(settings) as conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE batch_manifest SET status='LOADING', error=NULL, updated_at=now() WHERE batch_id=%s",
                    (batch_id,),
                )
            client = clickhouse_client(settings, "pipeline")
            for table, objects in grouped.items():
                if table not in RAW_COLUMNS:
                    raise ValueError(f"Raw table is not allow-listed: {table}")
                expected = sum(item[2] for item in objects)
                existing = client.query(
                    f"SELECT count() FROM raw.{table} WHERE batch_id = toUUID(%(batch_id)s)",
                    parameters={"batch_id": batch_id},
                ).first_row[0]
                if existing == expected:
                    continue
                if existing:
                    raise RuntimeError(
                        f"Partial immutable batch exists in raw.{table}: {existing}/{expected}"
                    )

                stage = f"raw_staging.{table}_{batch_id.replace('-', '')[:16]}"
                client.command(f"DROP TABLE IF EXISTS {stage}")
                client.command(f"CREATE TABLE {stage} AS raw.{table}")
                base_columns = RAW_COLUMNS[table]
                rows: list[list[Any]] = []
                for path, checksum, _ in objects:
                    for record in _download_rows(settings, path, checksum):
                        source_updated = record.get("updated_at") or record.get("occurred_at")
                        if source_updated is None:
                            raise ValueError(f"{table} record has no source timestamp")
                        rows.append(
                            [
                                *(_coerce(column, record.get(column)) for column in base_columns),
                                batch_id,
                                source_updated,
                                datetime.now(UTC),
                                _record_hash(record, base_columns),
                            ]
                        )
                audit_columns = ["batch_id", "source_updated_at", "ingested_at", "record_hash"]
                client.insert(stage, rows, column_names=[*base_columns, *audit_columns])
                staged = client.query(f"SELECT count() FROM {stage}").first_row[0]
                if staged != expected:
                    raise RuntimeError(f"Staging count mismatch for {table}: {staged}/{expected}")
                client.command(
                    f"INSERT INTO raw.{table} SELECT * FROM {stage}",
                    settings={"insert_deduplication_token": batch_id},
                )
                loaded = client.query(
                    f"SELECT count() FROM raw.{table} WHERE batch_id = toUUID(%(batch_id)s)",
                    parameters={"batch_id": batch_id},
                ).first_row[0]
                client.command(f"DROP TABLE {stage}")
                if loaded != expected:
                    raise RuntimeError(f"Raw count mismatch for {table}: {loaded}/{expected}")
            with control_connection(settings) as conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE batch_manifest SET status='LOADED', updated_at=now() WHERE batch_id=%s",
                    (batch_id,),
                )
            return True
        except Exception as error:
            with control_connection(settings) as conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE batch_manifest SET status='FAILED', error=%s, updated_at=now() WHERE batch_id=%s",
                    (str(error)[:4000], batch_id),
                )
            raise
        finally:
            lock_cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (f"load:{source}",))


def load_pending(settings: Settings) -> list[str]:
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT batch_id::text FROM batch_manifest WHERE status IN ('STORED','FAILED') ORDER BY created_at"
        )
        batch_ids = [row[0] for row in cur.fetchall()]
    for batch_id in batch_ids:
        load_batch(settings, batch_id)
    return batch_ids
