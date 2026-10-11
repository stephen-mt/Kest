#!/usr/bin/env python3
"""Idempotently seed the small amount of governance owned by the platform."""

from __future__ import annotations

import os

import httpx

BASE = "http://openmetadata-server:8585/api/v1"
HEADERS = {"Authorization": f"Bearer {os.environ['OPENMETADATA_INGESTION_TOKEN']}"}


def upsert(path: str, payload: dict[str, object]) -> dict[str, object]:
    response = httpx.put(f"{BASE}/{path}", headers=HEADERS, json=payload, timeout=20)
    response.raise_for_status()
    return response.json()


def tag_table(table_fqn: str, term_fqn: str) -> None:
    response = httpx.get(
        f"{BASE}/tables/name/{table_fqn}",
        headers=HEADERS,
        params={"fields": "tags"},
        timeout=20,
    )
    response.raise_for_status()
    table = response.json()
    tags = list(table.get("tags") or [])
    if any(tag.get("tagFQN") == term_fqn for tag in tags):
        return
    tags.append(
        {
            "tagFQN": term_fqn,
            "source": "Glossary",
            "labelType": "Manual",
            "state": "Confirmed",
        }
    )
    patch = httpx.patch(
        f"{BASE}/tables/{table['id']}",
        headers={**HEADERS, "Content-Type": "application/json-patch+json"},
        json=[{"op": "add", "path": "/tags", "value": tags}],
        timeout=20,
    )
    patch.raise_for_status()


def main() -> None:
    for name, description in (
        ("analytics-engineering", "Owns canonical commercial marts and data contracts."),
        ("growth-analytics", "Owns marketing funnel definitions and analysis."),
    ):
        upsert(
            "teams",
            {
                "name": name,
                "displayName": name.replace("-", " ").title(),
                "teamType": "Group",
                "description": description,
            },
        )

    upsert(
        "glossaries",
        {
            "name": "KestBusinessMetrics",
            "displayName": "Kest Business Metrics",
            "description": "Canonical business terms implemented by versioned dbt models.",
        },
    )
    for name, description in (
        ("NetRevenue", "Captured successful payments less successful refunds."),
        ("ConversionFunnel", "Unique sessions progressing through view, cart and checkout."),
    ):
        upsert(
            "glossaryTerms",
            {
                "glossary": "KestBusinessMetrics",
                "name": name,
                "description": description,
            },
        )

    tag_table(
        "kest_clickhouse.default.gold.mart_daily_revenue",
        "KestBusinessMetrics.NetRevenue",
    )
    tag_table(
        "kest_clickhouse.default.gold.mart_conversion_funnel",
        "KestBusinessMetrics.ConversionFunnel",
    )


if __name__ == "__main__":
    main()
