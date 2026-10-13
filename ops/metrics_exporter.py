#!/usr/bin/env python3
"""Expose low-cardinality platform SLO metrics and relay actionable alerts."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from kest_ingestion.config import Settings
from kest_ingestion.db import clickhouse_client, control_connection
from kest_ingestion.publish import MARTS


def labels(**values: object) -> str:
    encoded = []
    for key, value in values.items():
        escaped = str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        encoded.append(f'{key}="{escaped}"')
    return "{" + ",".join(encoded) + "}"


def probe(url: str) -> int:
    try:
        with urllib.request.urlopen(url, timeout=4) as response:
            return int(response.status < 500)
    except (OSError, urllib.error.HTTPError):
        return 0


def metrics() -> str:
    settings = Settings()
    lines = [
        "# HELP kest_platform_exporter_up Exporter collection succeeded",
        "# TYPE kest_platform_exporter_up gauge",
    ]
    services = {
        "airflow": "http://airflow-api-server:8080/api/v2/monitor/health",
        "superset": "http://superset:8088/health",
        "keycloak": "http://keycloak:9000/health/ready",
        "object_store": "http://object-store:9000/minio/health/ready",
    }
    if os.getenv("OPENMETADATA_REQUIRED", "false").lower() == "true":
        services["openmetadata"] = "http://openmetadata-server:8585/healthcheck"
    for name, url in services.items():
        lines.append(f"kest_service_up{labels(service=name)} {probe(url)}")

    try:
        with control_connection(settings) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT source,status,count(*),
                       max(extract(epoch from now()-updated_at))
                FROM batch_manifest
                WHERE status IN ('LOADING','FAILED','QUARANTINED')
                GROUP BY source,status
                """
            )
            for source, status, count, age in cur.fetchall():
                lines.append(
                    f"kest_batch_problem_total{labels(source=source,status=status)} {int(count)}"
                )
                lines.append(
                    f"kest_batch_problem_age_seconds{labels(source=source,status=status)} {float(age)}"
                )
            cur.execute(
                """
                SELECT run_type,status,coalesce(source,''),count(*),
                       max(extract(epoch from now()-coalesce(finished_at,started_at)))
                FROM pipeline_run
                WHERE started_at > now()-interval '24 hours'
                  AND status IN ('RUNNING','FAILED')
                GROUP BY run_type,status,coalesce(source,'')
                """
            )
            for run_type, status, source, count, age in cur.fetchall():
                problem_labels = labels(run_type=run_type, status=status, source=source)
                lines.append(
                    f"kest_pipeline_run_problem_total{problem_labels} {int(count)}"
                )
                lines.append(
                    f"kest_pipeline_run_problem_age_seconds{problem_labels} {float(age)}"
                )
            cur.execute(
                "SELECT source,extract(epoch from now()-updated_at) FROM source_checkpoint"
            )
            for source, age in cur.fetchall():
                lines.append(f"kest_source_checkpoint_age_seconds{labels(source=source)} {float(age)}")
            cur.execute(
                """
                SELECT dataset,extract(epoch from now()-max(committed_at))
                FROM publication_manifest WHERE status='COMMITTED' GROUP BY dataset
                """
            )
            for dataset, age in cur.fetchall():
                lines.append(f"kest_dataset_publication_age_seconds{labels(dataset=dataset)} {float(age)}")
            cur.execute(
                """
                SELECT status,count(*),max(extract(epoch from now()-created_at))
                FROM dataset_release
                WHERE status IN ('PREPARING','PARTIAL','FAILED')
                GROUP BY status
                """
            )
            for status, count, age in cur.fetchall():
                lines.append(
                    f"kest_release_problem_total{labels(status=status)} {int(count)}"
                )
                lines.append(
                    f"kest_release_problem_age_seconds{labels(status=status)} {float(age)}"
                )
            cur.execute("SELECT extract(epoch from now()-max(verified_at)) FROM backup_manifest")
            row = cur.fetchone()
            backup_age = row[0] if row and row[0] is not None else 1e12
            lines.append(f"kest_backup_verified_age_seconds {float(backup_age)}")

        client = clickhouse_client(settings, "da")
        for mart in MARTS:
            count = client.query(f"SELECT count() FROM gold.{mart}").first_row[0]
            lines.append(f"kest_gold_rows{labels(dataset=mart)} {count}")
        lines.append("kest_platform_exporter_up 1")
    except Exception as error:
        print(f"metrics_exporter: collection failed: {error}", flush=True)
        lines.append("kest_platform_exporter_up 0")
    return "\n".join(lines) + "\n"


def relay_alert(payload: bytes) -> None:
    webhook = os.getenv("PLATFORM_ALERT_WEBHOOK_URL")
    decoded: Any = json.loads(payload or b"{}")
    print(json.dumps({"alertmanager": decoded}, sort_keys=True), flush=True)
    if not webhook:
        return
    request = urllib.request.Request(
        webhook,
        data=json.dumps(decoded).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(request, timeout=10).close()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/metrics":
            payload = metrics().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
        elif self.path == "/health":
            payload = b"ok\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
        else:
            payload = b"not found\n"
            self.send_response(404)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/alerts":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        try:
            relay_alert(self.rfile.read(length))
        except Exception as error:
            self.send_error(502, str(error))
            return
        self.send_response(204)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        print(f"metrics_exporter: {format % args}", flush=True)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 9108), Handler).serve_forever()
