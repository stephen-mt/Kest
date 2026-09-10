import hashlib
import io
import json

import pyarrow as pa
from pyarrow import parquet

from kest.storage import list_objects, s3_client


def load_manifest(settings):
    client = s3_client(settings)
    key = f"{settings.bronze_prefix}/_manifest.json"
    payload = client.get_object(Bucket=settings.s3_bucket, Key=key)["Body"].read()
    return json.loads(payload), hashlib.sha256(payload).hexdigest()


def bronze_files(settings):
    client = s3_client(settings)
    prefix = settings.bronze_prefix + "/"
    result = {}
    for item in list_objects(client, settings.s3_bucket, prefix):
        if not item["Key"].endswith(".parquet"):
            continue
        table = item["Key"][len(prefix) :].split("/", 1)[0]
        result.setdefault(table, []).append(item)
    return {
        table: sorted(items, key=lambda item: item["Key"])
        for table, items in result.items()
    }


def parquet_schema(client, bucket, item):
    payload = client.get_object(Bucket=bucket, Key=item["Key"])["Body"].read()
    return parquet.ParquetFile(io.BytesIO(payload)).schema_arrow


def iceberg_arrow_schema(schema):
    fields = []
    for field in schema:
        field_type = (
            pa.string() if isinstance(field.type, pa.BaseExtensionType) else field.type
        )
        fields.append(pa.field(field.name, field_type, nullable=field.nullable))
    return pa.schema(fields)
