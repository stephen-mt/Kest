from __future__ import annotations

import csv
import hashlib
import json
import shutil
import time
import uuid
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, cast

import boto3
import dlt
import httpx
import pyarrow.parquet as pq
import yaml
from botocore.client import Config
from dlt.destinations import filesystem

from kest_ingestion.config import ROOT, Settings
from kest_ingestion.db import business_connection, clickhouse_client, control_connection
from kest_ingestion.models import BatchObject, BatchResult, SourceConfig, json_value

POSTGRES_TABLES = {
    "customers": ("customer_id", "updated_at"),
    "orders": ("order_id", "updated_at"),
    "order_items": ("order_item_id", "updated_at"),
    "payments": ("payment_id", "updated_at"),
    "refunds": ("refund_id", "updated_at"),
    "events": ("event_id", "occurred_at"),
}

DLT_COLUMN_HINTS: dict[str, dict[str, dict[str, Any]]] = {
    "customers": {"country": {"data_type": "text", "nullable": True}},
    "payments": {"succeeded_at": {"data_type": "timestamp", "nullable": True}},
    "refunds": {"succeeded_at": {"data_type": "timestamp", "nullable": True}},
    "events": {
        "customer_id": {"data_type": "text", "nullable": True},
        "product_id": {"data_type": "text", "nullable": True},
        "campaign_id": {"data_type": "text", "nullable": True},
    },
    "marketing_campaigns": {"ends_at": {"data_type": "timestamp", "nullable": True}},
    "product_catalog": {"unit_cost": {"data_type": "decimal", "nullable": True}},
}


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    return json_value(value)


def _content_identity(source: str, tables: dict[str, list[dict[str, Any]]]) -> tuple[str, str]:
    payload = json.dumps(_canonical(tables), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha256(payload.encode()).hexdigest()
    batch_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"kest:{source}:{digest}"))
    return batch_id, digest


def _checkpoint(settings: Settings, source: str) -> dict[str, Any]:
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute("SELECT watermark FROM source_checkpoint WHERE source = %s", (source,))
        row = cur.fetchone()
    return row[0] if row else {}


def _postgres_records(settings: Settings) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    checkpoint = _checkpoint(settings, "postgres")
    previous = checkpoint.get("updated_at")
    lower_bound = (
        datetime.fromisoformat(previous) - timedelta(hours=2)
        if previous
        else datetime(1970, 1, 1, tzinfo=UTC)
    )
    result: dict[str, list[dict[str, Any]]] = {}
    max_updated = lower_bound
    with business_connection(settings) as conn, conn.cursor() as cur:
        for table, (pk, cursor_column) in POSTGRES_TABLES.items():
            cur.execute(
                f'SELECT * FROM "{table}" WHERE "{cursor_column}" >= %s ORDER BY "{cursor_column}", "{pk}"',
                (lower_bound - (timedelta(hours=46) if table == "events" else timedelta()),),
            )
            names = [column.name for column in cur.description]
            rows = [dict(zip(names, row, strict=True)) for row in cur.fetchall()]
            result[table] = rows
            for row in rows:
                updated = row["updated_at"]
                if updated > max_updated:
                    max_updated = updated
    return result, {"updated_at": max_updated.isoformat()}


def _load_source_config(path: Path, settings: Settings) -> SourceConfig:
    raw = yaml.safe_load(path.read_text())
    raw["base_url"] = settings.source_api_url
    return SourceConfig.model_validate(raw)


def _rest_records(
    config: SourceConfig, checkpoint: dict[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    updated_since = checkpoint.get("updated_at")
    cursor: str | None = None
    records: list[dict[str, Any]] = []
    newest = updated_since
    with httpx.Client(timeout=config.timeout_seconds) as client:
        while True:
            params: dict[str, Any] = {"limit": config.page_size}
            if cursor:
                params[config.cursor_param] = cursor
            if updated_since:
                params["updated_since"] = updated_since
            for attempt in range(config.max_retries + 1):
                try:
                    response = client.get(f"{config.base_url}{config.endpoint}", params=params)
                    if response.status_code == 429:
                        if attempt == config.max_retries:
                            response.raise_for_status()
                        time.sleep(float(response.headers.get("Retry-After", "1")))
                        continue
                    response.raise_for_status()
                    break
                except (httpx.TimeoutException, httpx.TransportError):
                    if attempt == config.max_retries:
                        raise
                    time.sleep(min(2**attempt, 8))
            body = response.json()
            page = body[config.data_path]
            records.extend(page)
            for record in page:
                value = record.get(config.updated_at)
                if value and (newest is None or value > newest):
                    newest = value
            cursor = body.get(config.next_cursor_path)
            if not cursor:
                break
    return records, {"updated_at": newest} if newest else checkpoint


def _csv_records(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    records: list[dict[str, Any]] = []
    with path.open(newline="") as handle:
        for line, row in enumerate(csv.DictReader(handle), start=2):
            missing = {"product_id", "product_name", "category", "unit_cost", "updated_at"} - row.keys()
            if missing:
                raise ValueError(f"CSV contract failed at line {line}: missing {sorted(missing)}")
            try:
                cost = Decimal(row["unit_cost"]) if row["unit_cost"] else None
            except InvalidOperation as error:
                raise ValueError(f"CSV contract failed at line {line}: unit_cost is not numeric") from error
            records.append(
                {
                    "product_id": row["product_id"],
                    "product_name": row["product_name"],
                    "category": row["category"],
                    "unit_cost": cost,
                    "updated_at": datetime.fromisoformat(row["updated_at"].replace("Z", "+00:00")),
                    "is_deleted": row.get("is_deleted", "false").lower() == "true",
                }
            )
    return records, {"path": str(path.relative_to(ROOT)), "sha256": checksum}


def _s3_client(settings: Settings, *, writer: bool = True):
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_writer_key if writer else settings.s3_reader_key,
        aws_secret_access_key=settings.s3_writer_secret if writer else settings.s3_reader_secret,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def ensure_raw_bucket(settings: Settings) -> None:
    client = _s3_client(settings)
    try:
        client.head_bucket(Bucket="raw")
    except Exception:
        client.create_bucket(Bucket="raw")


def _as_resource(table_name: str, rows: Iterable[dict[str, Any]]):
    @dlt.resource(
        name=table_name,
        write_disposition="append",
        columns=cast(Any, DLT_COLUMN_HINTS.get(table_name)),
    )
    def records() -> Iterator[dict[str, Any]]:
        yield from rows

    return records()


def _write_and_upload(
    settings: Settings,
    source: str,
    batch_id: str,
    tables: dict[str, list[dict[str, Any]]],
) -> tuple[str, list[BatchObject]]:
    staging_root = settings.runtime_dir / "batches" / batch_id
    if staging_root.exists():
        shutil.rmtree(staging_root)
    staging_root.mkdir(parents=True)
    pipeline = dlt.pipeline(
        pipeline_name=f"kest_{source}_{batch_id.replace('-', '')[:12]}",
        pipelines_dir=str(settings.runtime_dir / "dlt"),
        destination=filesystem(bucket_url=str(staging_root), layout="{table_name}/{load_id}.{file_id}.{ext}"),
        dataset_name=source,
    )
    resources = [_as_resource(table, rows) for table, rows in tables.items() if rows]
    if resources:
        pipeline.run(resources, loader_file_format="parquet")

    client = _s3_client(settings)
    objects: list[BatchObject] = []
    for local_path in sorted(staging_root.rglob("*.parquet")):
        table_name = local_path.parent.name
        key = f"{source}/{table_name}/{batch_id}/{local_path.name}"
        body = local_path.read_bytes()
        checksum = hashlib.sha256(body).hexdigest()
        client.put_object(
            Bucket="raw", Key=key, Body=body, Metadata={"sha256": checksum, "batch-id": batch_id}
        )
        head = client.head_object(Bucket="raw", Key=key)
        if head["ContentLength"] != len(body) or head.get("Metadata", {}).get("sha256") != checksum:
            raise RuntimeError(f"Object verification failed: s3://raw/{key}")
        objects.append(
            BatchObject(
                table_name=table_name,
                object_path=key,
                checksum=checksum,
                size_bytes=len(body),
                row_count=pq.ParquetFile(local_path).metadata.num_rows,
            )
        )
    return f"s3://raw/{source}", objects


def _existing_batch(settings: Settings, batch_id: str) -> BatchResult | None:
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT source, object_prefix, checksum, row_count, watermark FROM batch_manifest WHERE batch_id = %s",
            (batch_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        cur.execute(
            "SELECT table_name, object_path, checksum, size_bytes, row_count FROM batch_objects WHERE batch_id = %s ORDER BY object_path",
            (batch_id,),
        )
        objects = [
            BatchObject(table_name=r[0], object_path=r[1], checksum=r[2], size_bytes=r[3], row_count=r[4])
            for r in cur.fetchall()
        ]
    return BatchResult(
        batch_id=batch_id,
        source=row[0],
        object_prefix=row[1],
        checksum=row[2],
        row_count=row[3],
        watermark=row[4],
        objects=objects,
        reused=True,
    )


def ingest_tables(
    settings: Settings,
    source: str,
    tables: dict[str, list[dict[str, Any]]],
    watermark: dict[str, Any],
) -> BatchResult | None:
    if not any(tables.values()):
        return None
    batch_id, aggregate = _content_identity(source, tables)
    existing = _existing_batch(settings, batch_id)
    if existing:
        return existing
    ensure_raw_bucket(settings)
    prefix, objects = _write_and_upload(settings, source, batch_id, tables)
    if sum(item.row_count for item in objects) != sum(len(rows) for rows in tables.values()):
        raise RuntimeError("Parquet row-count verification failed")
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO batch_manifest(batch_id, source, object_prefix, checksum, row_count, watermark, status) VALUES (%s,%s,%s,%s,%s,%s,'STORED')",
            (
                batch_id,
                source,
                prefix,
                aggregate,
                sum(item.row_count for item in objects),
                json.dumps(watermark),
            ),
        )
        cur.executemany(
            "INSERT INTO batch_objects(batch_id, table_name, object_path, checksum, size_bytes, row_count) VALUES (%s,%s,%s,%s,%s,%s)",
            [
                (batch_id, item.table_name, item.object_path, item.checksum, item.size_bytes, item.row_count)
                for item in objects
            ],
        )
        cur.execute(
            "INSERT INTO source_checkpoint(source, watermark, batch_id) VALUES (%s,%s,%s) ON CONFLICT(source) DO UPDATE SET watermark=excluded.watermark, batch_id=excluded.batch_id, updated_at=now()",
            (source, json.dumps(watermark), batch_id),
        )
    return BatchResult(
        batch_id=batch_id,
        source=source,
        object_prefix=prefix,
        checksum=aggregate,
        row_count=sum(item.row_count for item in objects),
        watermark=watermark,
        objects=objects,
    )


def ingest_postgres(settings: Settings) -> BatchResult | None:
    tables, watermark = _postgres_records(settings)
    return ingest_tables(settings, "postgres", tables, watermark)


def ingest_rest(settings: Settings, config_path: Path) -> BatchResult | None:
    config = _load_source_config(config_path, settings)
    checkpoint = _checkpoint(settings, config.name)
    rows, watermark = _rest_records(config, checkpoint)
    return ingest_tables(settings, config.name, {config.table_name: rows}, watermark)


def ingest_csv(settings: Settings, path: Path | None = None) -> BatchResult | None:
    fixture = path or ROOT / "ingestion" / "fixtures" / "product_catalog.csv"
    rows, watermark = _csv_records(fixture)
    checkpoint = _checkpoint(settings, "product_catalog")
    if checkpoint.get("sha256") == watermark["sha256"]:
        return None
    return ingest_tables(settings, "product_catalog", {"product_catalog": rows}, watermark)


def ingest_all(settings: Settings) -> list[BatchResult]:
    # Every validated YAML file is a source. Adding a REST source does not
    # require editing orchestration code.
    paths = sorted(settings.source_config_dir.glob("*.yml"))
    results = [
        ingest_postgres(settings),
        *(ingest_rest(settings, path) for path in paths),
        ingest_csv(settings),
    ]
    return [result for result in results if result is not None]


def reconcile_hard_deletes(settings: Settings) -> BatchResult | None:
    """Emit audited tombstones for keys removed without a soft-delete update."""
    client = clickhouse_client(settings, "pipeline")
    tombstones: dict[str, list[dict[str, Any]]] = {}
    now = datetime.now(UTC)
    with business_connection(settings) as source_conn, source_conn.cursor() as source_cur:
        for table, (primary_key, _) in POSTGRES_TABLES.items():
            source_cur.execute(f'SELECT "{primary_key}" FROM "{table}"')
            live_keys = {str(row[0]) for row in source_cur.fetchall()}
            rows = client.query(
                f"SELECT {primary_key} FROM raw.{table} GROUP BY {primary_key} "
                "HAVING argMax(is_deleted, tuple(source_updated_at, ingested_at, record_hash)) = 0"
            ).result_rows
            missing = sorted({str(row[0]) for row in rows} - live_keys)
            for key in missing:
                columns = [
                    row[0]
                    for row in client.query(f"DESCRIBE TABLE raw.{table}").result_rows
                    if row[0] not in {"batch_id", "source_updated_at", "ingested_at", "record_hash"}
                ]
                latest = client.query(
                    f"SELECT {', '.join(columns)} FROM raw.{table} "
                    f"WHERE {primary_key} = %(key)s "
                    "ORDER BY source_updated_at DESC, ingested_at DESC, record_hash DESC LIMIT 1",
                    parameters={"key": key},
                ).first_row
                record = dict(zip(columns, latest, strict=True))
                record["is_deleted"] = True
                record["updated_at"] = now
                tombstones.setdefault(table, []).append(record)
    return ingest_tables(
        settings,
        "postgres_reconciliation",
        tombstones,
        {"reconciled_at": now.isoformat(), "deleted_keys": sum(map(len, tombstones.values()))},
    ) if tombstones else None
