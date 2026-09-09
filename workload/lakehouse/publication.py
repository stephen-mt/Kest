import json

from botocore.exceptions import ClientError

from workload.core.storage import s3_client
from workload.landing.commits import put_immutable


def batch_prefix(settings):
    return f"{settings.iceberg_prefix}/_kest_batches"


def current_pointer_key(settings):
    return f"{batch_prefix(settings)}/current.json"


def load_current_pointer(settings, required=True):
    client = s3_client(settings)
    key = current_pointer_key(settings)
    try:
        response = client.get_object(Bucket=settings.s3_bucket, Key=key)
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if not required and status == 404:
            return None, None
        raise
    return json.loads(response["Body"].read()), response["ETag"].strip('"')


def write_batch_manifest(settings, batch_id, manifest):
    payload = json.dumps(manifest, indent=2, sort_keys=True).encode()
    key = f"{batch_prefix(settings)}/{batch_id}.json"
    put_immutable(
        s3_client(settings),
        settings.s3_bucket,
        key,
        payload,
        ContentType="application/json",
    )
    return key


def publish_pointer(settings, pointer, expected_etag):
    client = s3_client(settings)
    arguments = {
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
        client.put_object(**arguments)
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status in {409, 412}:
            raise RuntimeError(
                "Another batch changed the current lakehouse pointer"
            ) from None
        raise
