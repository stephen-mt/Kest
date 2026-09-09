"""Initialize one empty MinIO bucket and Lakekeeper warehouse at startup."""

import json
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LAKEKEEPER_HOST_URL = "http://127.0.0.1:8181"


def compose(*args, **kwargs):
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=PROJECT_ROOT,
        check=True,
        text=True,
        **kwargs,
    )


def request_json(base_url, path, payload=None, method=None):
    request = urllib.request.Request(
        base_url + path,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
            return json.loads(body) if body else None
    except urllib.error.HTTPError as exc:
        # A server error body might contain credentials; report only its status.
        raise SystemExit(
            f"Lakekeeper bootstrap failed: {path} returned HTTP {exc.code}"
        ) from None
    except urllib.error.URLError:
        raise SystemExit(
            "Lakekeeper is unreachable; check its health and logs."
        ) from None


def ensure_bucket():
    compose(
        "exec",
        "-T",
        "minio",
        "sh",
        "-ec",
        "mc alias set kest-local http://127.0.0.1:9000 "
        '"$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null; '
        'mc mb --ignore-existing "kest-local/$MINIO_BUCKET" >/dev/null; '
        'mc version enable "kest-local/$MINIO_BUCKET" >/dev/null',
    )


def ensure_warehouse(base_url, name, iceberg_prefix, minio):
    info = request_json(base_url, "/management/v1/info")
    if not info["bootstrapped"]:
        request_json(
            base_url, "/management/v1/bootstrap", {"accept-terms-of-use": True}
        )
        info = request_json(base_url, "/management/v1/info")

    project_id = info["default-project-id"]
    profile = {
        "type": "s3",
        "bucket": minio["MINIO_BUCKET"],
        "key-prefix": iceberg_prefix,
        "endpoint": "http://minio:9000/",
        "region": "us-east-1",
        "path-style-access": True,
        "flavor": "s3-compat",
        "sts-enabled": False,
    }
    query = urllib.parse.urlencode({"project-id": project_id})
    warehouses = request_json(base_url, f"/management/v1/warehouse?{query}")[
        "warehouses"
    ]
    existing = next(
        (warehouse for warehouse in warehouses if warehouse["name"] == name), None
    )
    if existing:
        actual = existing["storage-profile"]
        if any(actual.get(key) != value for key, value in profile.items()):
            warehouse_id = existing["warehouse-id"]
            namespaces = request_json(
                base_url, f"/catalog/v1/{warehouse_id}/namespaces"
            )["namespaces"]
            if namespaces:
                raise SystemExit(
                    "Existing warehouse storage differs from .env and contains namespaces; "
                    "refusing to replace it."
                )
            request_json(
                base_url,
                f"/management/v1/warehouse/{warehouse_id}",
                method="DELETE",
            )
            existing = None
            print(f"Replaced empty Lakekeeper warehouse {name} storage profile.")
        else:
            print(f"Lakekeeper warehouse {name} already configured; preserved.")
            return

    request_json(
        base_url,
        "/management/v1/warehouse",
        {
            "warehouse-name": name,
            "project-id": project_id,
            "storage-profile": profile,
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "access-key-id": minio["MINIO_ROOT_USER"],
                "secret-access-key": minio["MINIO_ROOT_PASSWORD"],
            },
        },
    )
    print(f"Created empty Lakekeeper warehouse {name} backed by MinIO.")


def main():
    config = json.loads(
        compose("config", "--format", "json", capture_output=True).stdout
    )
    minio = config["services"]["minio"]["environment"]
    lakekeeper = config["services"]["lakekeeper"]["environment"]
    ensure_bucket()
    ensure_warehouse(
        LAKEKEEPER_HOST_URL,
        lakekeeper["LAKEKEEPER_WAREHOUSE"],
        lakekeeper["ICEBERG_PREFIX"],
        minio,
    )


if __name__ == "__main__":
    main()
