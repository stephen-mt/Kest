import hashlib

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def s3_client(settings):
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def list_objects(client, bucket, prefix):
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents", [])


def sha256(payload):
    return hashlib.sha256(payload).hexdigest()


def put_immutable(client, bucket, key, payload, **kwargs):
    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=payload,
            IfNoneMatch="*",
            **kwargs,
        )
        return
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status not in {409, 412}:
            raise

    existing = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    if existing != payload:
        raise RuntimeError(f"Immutable object differs on retry: {key}")
