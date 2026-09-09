import gzip
import hashlib
import json

from botocore.exceptions import ClientError

from workload.core.storage import list_objects, s3_client


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
        raise RuntimeError(f"Immutable landing object differs on retry: {key}")


def commit_prefix(settings):
    return f"{settings.landing_prefix}/commits/"


def load_commits(settings):
    client = s3_client(settings)
    commits = []
    for item in list_objects(client, settings.s3_bucket, commit_prefix(settings)):
        if not item["Key"].endswith(".json"):
            continue
        body = client.get_object(Bucket=settings.s3_bucket, Key=item["Key"])[
            "Body"
        ].read()
        commit = json.loads(body)
        commit["_key"] = item["Key"]
        commits.append(commit)
    return sorted(commits, key=lambda value: value["source"]["first_lsn_int"])


def load_committed_events(settings):
    client = s3_client(settings)
    events = {}
    commits = load_commits(settings)
    for commit in commits:
        observed = 0
        for item in commit["objects"]:
            body = client.get_object(Bucket=settings.s3_bucket, Key=item["key"])[
                "Body"
            ].read()
            if sha256(body) != item["sha256"]:
                raise RuntimeError(f"Landing checksum differs: {item['key']}")
            lines = gzip.decompress(body).splitlines()
            if len(lines) != item["event_count"]:
                raise RuntimeError(f"Landing event count differs: {item['key']}")
            observed += len(lines)
            for line in lines:
                event = json.loads(line)
                previous = events.setdefault(event["event_id"], event)
                if previous != event:
                    raise RuntimeError(
                        f"CDC event ID has conflicting payloads: {event['event_id']}"
                    )
        if observed != commit["event_count"]:
            raise RuntimeError(f"CDC commit count differs: {commit['_key']}")
    return list(events.values()), commits
