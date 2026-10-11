#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from kest_ingestion.config import Settings
from kest_ingestion.ingest import _s3_client


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def backup_raw(destination: Path) -> None:
    settings = Settings()
    client = _s3_client(settings, writer=False)
    raw_dir = destination / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    objects: list[dict[str, object]] = []
    total_bytes = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket="raw"):
        for item in page.get("Contents", []):
            key = item["Key"]
            if key.startswith("platform-backups/"):
                continue
            target = raw_dir / key
            target.parent.mkdir(parents=True, exist_ok=True)
            body = client.get_object(Bucket="raw", Key=key)["Body"].read()
            target.write_bytes(body)
            total_bytes += len(body)
            objects.append({"key": key, "size": len(body), "sha256": hashlib.sha256(body).hexdigest()})
    (destination / "raw-manifest.json").write_text(json.dumps(objects, indent=2, sort_keys=True))
    print(json.dumps({"objects": len(objects), "bytes": total_bytes}))


def restore_raw(source: Path, bucket: str) -> None:
    settings = Settings()
    writer = _s3_client(settings)
    try:
        writer.head_bucket(Bucket=bucket)
    except Exception:
        writer.create_bucket(Bucket=bucket)
    manifest = json.loads((source / "raw-manifest.json").read_text())
    for item in manifest:
        path = source / "raw" / item["key"]
        body = path.read_bytes()
        actual = hashlib.sha256(body).hexdigest()
        if actual != item["sha256"]:
            raise RuntimeError(f"Local raw backup checksum mismatch: {item['key']}")
        writer.put_object(Bucket=bucket, Key=item["key"], Body=body, Metadata={"sha256": actual})
        restored = writer.get_object(Bucket=bucket, Key=item["key"])["Body"].read()
        if hashlib.sha256(restored).hexdigest() != actual:
            raise RuntimeError(f"Restored raw checksum mismatch: {item['key']}")
    print(json.dumps({"bucket": bucket, "verified_objects": len(manifest)}))


def write_checksums(destination: Path) -> None:
    excluded = {"SHA256SUMS", "backup.json"}
    paths = sorted(path for path in destination.rglob("*") if path.is_file() and path.name not in excluded)
    lines = [f"{sha256(path)}  {path.relative_to(destination)}" for path in paths]
    (destination / "SHA256SUMS").write_text("\n".join(lines) + "\n")


def verify_checksums(source: Path) -> None:
    for line in (source / "SHA256SUMS").read_text().splitlines():
        expected, relative = line.split("  ", 1)
        if sha256(source / relative) != expected:
            raise RuntimeError(f"Backup checksum mismatch: {relative}")
    print("backup artifact checksums: ok")


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    backup = sub.add_parser("backup-raw")
    backup.add_argument("destination", type=Path)
    restore = sub.add_parser("restore-raw")
    restore.add_argument("source", type=Path)
    restore.add_argument("bucket")
    checksums = sub.add_parser("write-checksums")
    checksums.add_argument("destination", type=Path)
    verify = sub.add_parser("verify-checksums")
    verify.add_argument("source", type=Path)
    args = parser.parse_args()
    if args.command == "backup-raw":
        backup_raw(args.destination)
    elif args.command == "restore-raw":
        restore_raw(args.source, args.bucket)
    elif args.command == "write-checksums":
        write_checksums(args.destination)
    else:
        verify_checksums(args.source)


if __name__ == "__main__":
    main()
