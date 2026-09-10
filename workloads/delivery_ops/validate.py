"""Validate source contracts, published tables, quality, and governance artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import psycopg

from workloads.delivery_ops.batch import GOLD_QUERIES, SILVER_QUERIES, SOURCE_TABLES
from workloads.delivery_ops.config import Settings
from workloads.delivery_ops.lakehouse import (
    iceberg_catalog,
    load_current_pointer,
    s3_client,
)

CONTRACT = Path(__file__).with_name("contracts") / "source_contract.json"


def load_json_object(settings: Settings, key: str) -> dict[str, Any]:
    response = s3_client(settings).get_object(Bucket=settings.s3_bucket, Key=key)
    return json.loads(response["Body"].read())


def check_source(settings: Settings) -> dict[str, Any]:
    contract = json.loads(CONTRACT.read_text())
    with psycopg.connect(**settings.pg_kwargs()) as connection:
        table_rows = {
            table: connection.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
            for table in SOURCE_TABLES
        }
        duplicates = connection.execute(
            """
            SELECT count(*) FROM (
                SELECT partner_event_id FROM shipment_events
                GROUP BY partner_event_id HAVING count(*) > 1
            ) duplicate
            """
        ).fetchone()[0]
        orphans = connection.execute(
            """
            SELECT count(*) FROM shipment_events e
            LEFT JOIN shipments s USING (shipment_id)
            WHERE s.shipment_id IS NULL
            """
        ).fetchone()[0]
        clock_skew = connection.execute(
            "SELECT count(*) FROM shipment_events WHERE recorded_at < event_at"
        ).fetchone()[0]
        source_columns = {
            f"{table}.{column}"
            for table, column in connection.execute(
                """
                SELECT table_name, column_name FROM information_schema.columns
                WHERE table_schema = 'public'
                """
            ).fetchall()
        }
    missing_pii = set(contract["pii_columns"]) - source_columns
    if missing_pii:
        raise RuntimeError(f"Contract PII columns are missing: {sorted(missing_pii)}")
    if set(contract["tables"]) - set(table_rows):
        raise RuntimeError("Contract references a source table that does not exist")
    if not all(table_rows.values()):
        raise RuntimeError(f"Source contains empty required tables: {table_rows}")
    return {
        "clock_skew_events": clock_skew,
        "contract_version": contract["contract_version"],
        "duplicate_keys": duplicates,
        "orphan_events": orphans,
        "table_rows": table_rows,
    }


def check_lakehouse(settings: Settings) -> dict[str, Any]:
    pointer, _ = load_current_pointer(settings)
    catalog = iceberg_catalog(settings)
    expected = {
        "silver": set(SILVER_QUERIES),
        "gold": set(GOLD_QUERIES),
    }
    rows: dict[str, dict[str, int]] = {}
    for layer in ("silver", "gold"):
        namespace = pointer[f"{layer}_namespace"]
        tables = {identifier[-1] for identifier in catalog.list_tables((namespace,))}
        if tables != expected[layer]:
            raise RuntimeError(
                f"{namespace} tables differ: actual={sorted(tables)} expected={sorted(expected[layer])}"
            )
        rows[layer] = {}
        for table_name in sorted(tables):
            table = catalog.load_table((namespace, table_name))
            rows[layer][table_name] = sum(
                task.file.record_count for task in table.scan().plan_files()
            )

    quality = load_json_object(settings, pointer["quality_key"])
    assets = load_json_object(settings, pointer["assets_key"])
    lineage = load_json_object(settings, pointer["lineage_key"])
    manifest = load_json_object(settings, pointer["manifest_key"])
    if quality["status"] != "passed":
        raise RuntimeError(f"Published quality result failed: {quality}")
    if (
        lineage["eventType"] != "COMPLETE"
        or lineage["run"]["runId"] != pointer["batch_id"]
    ):
        raise RuntimeError("OpenLineage run does not match the current pointer")
    if manifest["batch_id"] != pointer["batch_id"]:
        raise RuntimeError("Manifest does not match the current pointer")
    if len(assets["assets"]) != sum(len(layer) for layer in manifest["rows"].values()):
        raise RuntimeError("Asset inventory is incomplete")
    return {
        "batch_id": pointer["batch_id"],
        "governed_assets": len(assets["assets"]),
        "namespaces": {
            "silver": pointer["silver_namespace"],
            "gold": pointer["gold_namespace"],
        },
        "quality": quality,
        "rows": rows,
    }


def run(settings: Settings) -> dict[str, Any]:
    result = {"source": check_source(settings), "lakehouse": check_lakehouse(settings)}
    print(json.dumps(result, indent=2, sort_keys=True))
    return result
