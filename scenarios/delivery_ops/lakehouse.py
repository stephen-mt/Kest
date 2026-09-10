"""Iceberg, object storage, and atomic publication helpers."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import boto3
import pyarrow as pa
from botocore.config import Config
from botocore.exceptions import ClientError
from pyiceberg.catalog import Catalog, load_catalog

from scenarios.delivery_ops.config import Settings


def s3_client(settings: Settings):
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def iceberg_catalog(settings: Settings) -> Catalog:
    return load_catalog(settings.iceberg_catalog)


def ensure_namespace(catalog: Catalog, namespace: str) -> None:
    identifier = (namespace,)
    if identifier not in catalog.list_namespaces():
        catalog.create_namespace(
            identifier,
            properties={
                "owner": "delivery-data@kest.local",
                "domain": "logistics",
                "data-product": "delivery-operations",
            },
        )


def remove_namespace(catalog: Catalog, namespace: str) -> None:
    identifier = (namespace,)
    if identifier not in catalog.list_namespaces():
        return
    for table in catalog.list_tables(identifier):
        catalog.purge_table(table)
    catalog.drop_namespace(identifier)


def write_table(
    catalog: Catalog,
    namespace: str,
    name: str,
    data: pa.Table,
    properties: dict[str, str],
) -> int:
    identifier = (namespace, name)
    if catalog.table_exists(identifier):
        table = catalog.load_table(identifier)
        actual = sum(task.file.record_count for task in table.scan().plan_files())
        if actual != data.num_rows:
            raise RuntimeError(
                f"{namespace}.{name}: existing retry output has {actual} rows, "
                f"expected {data.num_rows}"
            )
        return actual
    table = catalog.create_table(
        identifier,
        schema=data.schema,
        properties={
            "write.parquet.compression-codec": "zstd",
            "write.target-file-size-bytes": str(64 * 1024**2),
            **properties,
        },
    )
    if data.num_rows:
        table.append(data, snapshot_properties={"kest.job": "delivery_ops_batch"})
    table.refresh()
    actual = sum(task.file.record_count for task in table.scan().plan_files())
    if actual != data.num_rows:
        raise RuntimeError(
            f"{namespace}.{name}: wrote {actual}, expected {data.num_rows}"
        )
    return actual


def object_prefix(settings: Settings) -> str:
    return settings.iceberg_prefix.strip("/") + "/_control"


def current_pointer_key(settings: Settings) -> str:
    return f"{object_prefix(settings)}/current.json"


def load_current_pointer(
    settings: Settings, required: bool = True
) -> tuple[dict[str, Any] | None, str | None]:
    try:
        response = s3_client(settings).get_object(
            Bucket=settings.s3_bucket, Key=current_pointer_key(settings)
        )
    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if not required and status == 404:
            return None, None
        raise
    return json.loads(response["Body"].read()), response["ETag"].strip('"')


def put_immutable_json(settings: Settings, key: str, value: dict[str, Any]) -> str:
    payload = json.dumps(value, indent=2, sort_keys=True).encode()
    digest = hashlib.sha256(payload).hexdigest()
    try:
        s3_client(settings).put_object(
            Bucket=settings.s3_bucket,
            Key=key,
            Body=payload,
            ContentType="application/json",
            Metadata={"sha256": digest},
            IfNoneMatch="*",
        )
    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status not in {409, 412}:
            raise
        existing = (
            s3_client(settings)
            .get_object(Bucket=settings.s3_bucket, Key=key)["Body"]
            .read()
        )
        if existing != payload:
            raise RuntimeError(
                f"Immutable object conflict: s3://{settings.s3_bucket}/{key}"
            ) from None
    return digest


def publish_pointer(
    settings: Settings, pointer: dict[str, Any], expected_etag: str | None
) -> None:
    arguments: dict[str, Any] = {
        "Bucket": settings.s3_bucket,
        "Key": current_pointer_key(settings),
        "Body": json.dumps(pointer, indent=2, sort_keys=True).encode(),
        "ContentType": "application/json",
    }
    if expected_etag is None:
        arguments["IfNoneMatch"] = "*"
    else:
        arguments["IfMatch"] = expected_etag
    try:
        s3_client(settings).put_object(**arguments)
    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status in {409, 412}:
            raise RuntimeError(
                "Another DeliveryOps batch changed current.json"
            ) from None
        raise


def control_key(settings: Settings, category: str, batch_id: str) -> str:
    return f"{object_prefix(settings)}/{category}/{batch_id}.json"


def object_description(settings: Settings, key: str) -> str:
    return f"s3://{settings.s3_bucket}/{key}"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
