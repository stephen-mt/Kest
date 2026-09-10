"""Governance artifacts emitted by the DeliveryOps data product."""

from __future__ import annotations

from typing import Any

PRODUCER = "https://github.com/stephen-mt/Kest"


def classification_for(table: str) -> str:
    restricted_tokens = ("customer", "address", "delivery_attempt")
    return (
        "restricted"
        if any(token in table for token in restricted_tokens)
        else "internal"
    )


def asset_inventory(
    batch_id: str,
    namespaces: dict[str, str],
    rows: dict[str, dict[str, int]],
) -> dict[str, Any]:
    assets = []
    for layer, namespace in namespaces.items():
        for table, row_count in rows[layer].items():
            assets.append(
                {
                    "asset": f"{namespace}.{table}",
                    "classification": classification_for(table),
                    "data_product": "delivery-operations",
                    "domain": "logistics",
                    "layer": layer,
                    "owner": "delivery-data@kest.local",
                    "row_count": row_count,
                    "tags": ["delivery", "iceberg", layer],
                }
            )
    return {"batch_id": batch_id, "assets": assets, "schema_version": 1}


def openlineage_event(
    batch_id: str,
    event_time: str,
    source_tables: list[str],
    outputs: list[str],
) -> dict[str, Any]:
    return {
        "eventType": "COMPLETE",
        "eventTime": event_time,
        "producer": PRODUCER,
        "schemaURL": (
            "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"
        ),
        "run": {"runId": batch_id},
        "job": {
            "namespace": "kest.delivery_ops",
            "name": "delivery_ops_batch",
        },
        "inputs": [
            {
                "namespace": "postgresql://postgres-delivery:5432/delivery_ops",
                "name": f"public.{table}",
            }
            for table in source_tables
        ],
        "outputs": [
            {"namespace": "iceberg://kest-local", "name": table} for table in outputs
        ],
    }
