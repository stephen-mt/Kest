from __future__ import annotations

import os

import httpx


def main() -> None:
    token = os.environ["OPENMETADATA_INGESTION_TOKEN"]
    headers = {"Authorization": f"Bearer {token}"}
    base = "http://openmetadata-server:8585/api/v1"
    expected = ("mart_daily_revenue", "mart_conversion_funnel", "mart_customer_orders")
    for table in expected:
        response = httpx.get(f"{base}/tables/name/kest_clickhouse.gold.{table}", headers=headers, timeout=15)
        response.raise_for_status()
        payload = response.json()
        if not payload.get("columns"):
            raise RuntimeError(f"OpenMetadata table has no columns: {table}")
        if not payload.get("owner"):
            raise RuntimeError(f"OpenMetadata table has no owner: {table}")


if __name__ == "__main__":
    main()
