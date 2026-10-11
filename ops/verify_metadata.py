from __future__ import annotations

import json
import os
from typing import Any

import httpx

BASE = "http://openmetadata-server:8585/api/v1"
MARTS = {
    "mart_daily_revenue": {
        "owner": "analytics-engineering",
        "tag": "KestBusinessMetrics.NetRevenue",
    },
    "mart_conversion_funnel": {
        "owner": "growth-analytics",
        "tag": "KestBusinessMetrics.ConversionFunnel",
    },
    "mart_customer_orders": {"owner": "analytics-engineering"},
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def collection(client: httpx.Client, path: str, *, fields: str | None = None) -> list[dict[str, Any]]:
    params: dict[str, str | int] = {"limit": 100}
    if fields:
        params["fields"] = fields
    response = client.get(path, params=params)
    response.raise_for_status()
    return response.json().get("data", [])


def main() -> None:
    token = os.environ["OPENMETADATA_INGESTION_TOKEN"]
    headers = {"Authorization": f"Bearer {token}"}
    with httpx.Client(base_url=BASE, headers=headers, timeout=20) as client:
        test_cases = collection(client, "/dataQuality/testCases")
        test_fqns = {item["fullyQualifiedName"] for item in test_cases}

        verified_marts: list[str] = []
        for table, expectation in MARTS.items():
            fqn = f"kest_clickhouse.default.gold.{table}"
            response = client.get(f"/tables/name/{fqn}", params={"fields": "owners,tags,columns"})
            response.raise_for_status()
            payload = response.json()
            require(bool(payload.get("description")), f"OpenMetadata table has no description: {table}")
            require(bool(payload.get("columns")), f"OpenMetadata table has no columns: {table}")
            owners = {owner["name"] for owner in payload.get("owners", [])}
            require(expectation["owner"] in owners, f"Wrong or missing owner for {table}: {owners}")
            expected_tag = expectation.get("tag")
            tags = {tag["tagFQN"] for tag in payload.get("tags", [])}
            require(not expected_tag or expected_tag in tags, f"Missing glossary tag for {table}: {tags}")
            require(
                any(test.startswith(f"{fqn}.") for test in test_fqns),
                f"No dbt test case attached to {table}",
            )
            lineage = client.get(
                f"/lineage/table/{payload['id']}",
                params={"upstreamDepth": 3, "downstreamDepth": 1},
            )
            lineage.raise_for_status()
            require(bool(lineage.json().get("nodes")), f"No table lineage found for {table}")
            verified_marts.append(fqn)

        glossary_terms = {term["fullyQualifiedName"] for term in collection(client, "/glossaryTerms")}
        require(
            {
                "KestBusinessMetrics.NetRevenue",
                "KestBusinessMetrics.ConversionFunnel",
            }.issubset(glossary_terms),
            f"Missing business glossary terms: {glossary_terms}",
        )

        dashboards = {item.get("displayName") for item in collection(client, "/dashboards")}
        require(
            {"Executive Revenue & Orders", "Marketing Funnel & Spend"}.issubset(dashboards),
            f"Missing Superset dashboards: {dashboards}",
        )

        pipelines = {item["fullyQualifiedName"] for item in collection(client, "/pipelines")}
        expected_pipelines = {
            f"kest_airflow.{name}"
            for name in (
                "ecommerce_end_to_end",
                "source_ingestion",
                "transform_publish",
                "controlled_backfill",
                "metadata_sync",
            )
        }
        require(expected_pipelines.issubset(pipelines), f"Missing Airflow pipelines: {pipelines}")

        evidence = {
            "marts": verified_marts,
            "dbt_test_cases": len(test_cases),
            "glossary_terms": sorted(glossary_terms),
            "dashboards": sorted(name for name in dashboards if name),
            "pipelines": len(expected_pipelines),
            "lineage": "table-level verified; column-level is not claimed by this connector run",
        }
    print(json.dumps(evidence, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
