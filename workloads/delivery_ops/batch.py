"""Versioned Bronze, Silver, and Gold batch for DeliveryOps."""

from __future__ import annotations

import json
import re
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import duckdb
import psycopg
import pyarrow as pa
from botocore.exceptions import ClientError

from workloads.delivery_ops.config import Settings
from workloads.delivery_ops.governance import (
    asset_inventory,
    classification_for,
    openlineage_event,
)
from workloads.delivery_ops.lakehouse import (
    control_key,
    current_pointer_key,
    ensure_namespace,
    iceberg_catalog,
    load_current_pointer,
    publish_pointer,
    put_immutable_json,
    remove_namespace,
    s3_client,
    utc_now,
    write_table,
)

SOURCE_TABLES = (
    "hubs",
    "service_levels",
    "merchants",
    "merchant_contracts",
    "customers",
    "addresses",
    "couriers",
    "courier_assignments",
    "shipments",
    "shipment_items",
    "shipment_events",
    "delivery_attempts",
    "routes",
    "route_stops",
    "charges",
    "refunds",
)

SILVER_QUERIES = {
    "dim_merchant_scd2": """
        SELECT c.contract_id AS merchant_sk,
               m.merchant_id,
               trim(m.merchant_name) AS merchant_name,
               m.home_hub_id,
               c.merchant_tier,
               try_cast(c.discount_rate AS DECIMAL(7,4)) AS discount_rate,
               try_cast(c.credit_limit_vnd AS DECIMAL(18,2)) AS credit_limit_vnd,
               try_cast(c.valid_from AS TIMESTAMPTZ) AS valid_from,
               try_cast(c.valid_to AS TIMESTAMPTZ) AS valid_to,
               c.valid_to IS NULL AS is_current,
               m.deleted_at IS NOT NULL AS is_deleted
        FROM src_merchant_contracts c
        JOIN src_merchants m USING (merchant_id)
    """,
    "dim_courier_scd2": """
        SELECT a.assignment_id AS courier_sk,
               c.courier_id,
               trim(c.courier_name) AS courier_name,
               a.hub_id,
               a.vehicle_type,
               c.employment_status,
               try_cast(a.valid_from AS TIMESTAMPTZ) AS valid_from,
               try_cast(a.valid_to AS TIMESTAMPTZ) AS valid_to,
               a.valid_to IS NULL AS is_current,
               c.deleted_at IS NOT NULL AS is_deleted
        FROM src_courier_assignments a
        JOIN src_couriers c USING (courier_id)
    """,
    "dim_customer": """
        WITH normalized AS (
            SELECT *, regexp_replace(phone_raw, '[^0-9]', '', 'g') AS digits
            FROM src_customers
        )
        SELECT customer_id,
               trim(full_name) AS full_name,
               CASE
                 WHEN starts_with(digits, '84') THEN '0' || substr(digits, 3)
                 ELSE digits
               END AS phone_normalized,
               lower(trim(email_raw)) AS email_normalized,
               sha256(coalesce(lower(trim(email_raw)), '') || '|' || digits) AS pii_hash,
               try_cast(created_at AS TIMESTAMPTZ) AS created_at,
               try_cast(updated_at AS TIMESTAMPTZ) AS updated_at,
               deleted_at IS NOT NULL AS is_deleted
        FROM normalized
    """,
    "dim_location": """
        SELECT row_number() OVER (ORDER BY province_canonical, district_canonical)::BIGINT AS location_sk,
               province_canonical,
               district_canonical
        FROM (
            SELECT DISTINCT
              CASE lower(trim(province_raw))
                WHEN 'tp hcm' THEN 'Hồ Chí Minh'
                WHEN 'ho chi minh' THEN 'Hồ Chí Minh'
                WHEN 'ha noi' THEN 'Hà Nội'
                WHEN 'da nang' THEN 'Đà Nẵng'
                ELSE trim(province_raw)
              END AS province_canonical,
              nullif(trim(district_raw), '') AS district_canonical
            FROM src_addresses
        )
    """,
    "fact_shipment": """
        SELECT shipment_id, tracking_number, merchant_id, customer_id, address_id,
               origin_hub_id, destination_hub_id, service_level_code,
               current_status AS source_current_status,
               CASE lower(weight_unit)
                 WHEN 'kg' THEN try_cast(weight_value AS DECIMAL(18,3)) * 1000
                 WHEN 'g' THEN try_cast(weight_value AS DECIMAL(18,3))
               END AS weight_grams,
               try_cast(regexp_replace(declared_value_text, '[^0-9.-]', '', 'g') AS DECIMAL(18,2))
                 * CASE declared_currency WHEN 'USD' THEN 25000 WHEN 'THB' THEN 700 ELSE 1 END
                 AS declared_value_vnd,
               declared_currency,
               try_cast(created_at AS TIMESTAMPTZ) AS created_at,
               try_cast(promised_at AS TIMESTAMPTZ) AS promised_at,
               try_cast(updated_at AS TIMESTAMPTZ) AS updated_at,
               try_cast(cancelled_at AS TIMESTAMPTZ) AS cancelled_at,
               deleted_at IS NOT NULL AS is_deleted
        FROM src_shipments
    """,
    "fact_shipment_event": """
        SELECT try_cast(event_row_id AS BIGINT) AS event_row_id,
               partner_event_id, shipment_id, event_type,
               try_cast(event_at AS TIMESTAMPTZ) AS event_at,
               try_cast(recorded_at AS TIMESTAMPTZ) AS recorded_at,
               try_cast(source_version AS INTEGER) AS source_version,
               hub_id, courier_id,
               CASE lower(trim(reason_code_raw))
                 WHEN 'no_answer' THEN 'recipient_unavailable'
                 WHEN 'khong nghe may' THEN 'recipient_unavailable'
                 WHEN 'address_err' THEN 'invalid_address'
                 WHEN 'addr-not-found' THEN 'invalid_address'
                 ELSE nullif(lower(trim(reason_code_raw)), '')
               END AS reason_code,
               payload AS payload_json
        FROM src_shipment_events
        QUALIFY row_number() OVER (
            PARTITION BY partner_event_id
            ORDER BY try_cast(source_version AS INTEGER) DESC,
                     try_cast(recorded_at AS TIMESTAMPTZ) DESC,
                     try_cast(event_row_id AS BIGINT) DESC
        ) = 1
    """,
    "fact_delivery_attempt": """
        SELECT attempt_id, shipment_id, try_cast(attempt_number AS INTEGER) AS attempt_number,
               courier_id, try_cast(attempted_at AS TIMESTAMPTZ) AS attempted_at,
               outcome,
               CASE lower(trim(reason_code_raw))
                 WHEN 'no_answer' THEN 'recipient_unavailable'
                 WHEN 'khong nghe may' THEN 'recipient_unavailable'
                 WHEN 'address_err' THEN 'invalid_address'
                 ELSE nullif(lower(trim(reason_code_raw)), '')
               END AS reason_code,
               proof_url,
               try_cast(recorded_at AS TIMESTAMPTZ) AS recorded_at
        FROM src_delivery_attempts
    """,
    "fact_charge": """
        SELECT charge_id, shipment_id, charge_type,
               try_cast(regexp_replace(amount_text, '[^0-9.-]', '', 'g') AS DECIMAL(18,2))
                 * CASE currency WHEN 'USD' THEN 25000 WHEN 'THB' THEN 700 ELSE 1 END
                 AS amount_vnd,
               currency, charge_status,
               try_cast(charged_at AS TIMESTAMPTZ) AS charged_at,
               try_cast(updated_at AS TIMESTAMPTZ) AS updated_at
        FROM src_charges
    """,
    "fact_route_stop": """
        SELECT s.route_stop_id, s.route_id, s.shipment_id,
               try_cast(s.planned_sequence AS INTEGER) AS planned_sequence,
               try_cast(s.actual_sequence AS INTEGER) AS actual_sequence,
               try_cast(s.planned_arrival_at AS TIMESTAMPTZ) AS planned_arrival_at,
               try_cast(s.actual_arrival_at AS TIMESTAMPTZ) AS actual_arrival_at,
               s.stop_outcome, r.hub_id, r.courier_id,
               try_cast(r.route_date AS DATE) AS route_date
        FROM src_route_stops s JOIN src_routes r USING (route_id)
    """,
    "shipment_current_state": """
        SELECT shipment_id, partner_event_id AS current_event_id,
               event_type AS current_status, event_at AS current_event_at,
               recorded_at AS state_recorded_at, source_version,
               hub_id, courier_id
        FROM silver_fact_shipment_event e
        JOIN silver_fact_shipment s USING (shipment_id)
        QUALIFY row_number() OVER (
            PARTITION BY shipment_id
            ORDER BY source_version DESC, recorded_at DESC, event_row_id DESC
        ) = 1
    """,
    "dq_rejected_records": """
        WITH ranked AS (
            SELECT *, row_number() OVER (
                PARTITION BY partner_event_id
                ORDER BY try_cast(source_version AS INTEGER) DESC,
                         try_cast(recorded_at AS TIMESTAMPTZ) DESC,
                         try_cast(event_row_id AS BIGINT) DESC
            ) AS occurrence
            FROM src_shipment_events
        )
        SELECT 'shipment_events' AS source_table, partner_event_id AS record_id,
               'duplicate_partner_event' AS rule_code, 'warning' AS severity,
               recorded_at AS detected_at, payload AS raw_reference
        FROM ranked WHERE occurrence > 1
        UNION ALL
        SELECT 'shipment_events', e.partner_event_id, 'orphan_shipment', 'error',
               e.recorded_at, e.payload
        FROM src_shipment_events e
        LEFT JOIN src_shipments s USING (shipment_id)
        WHERE s.shipment_id IS NULL
        UNION ALL
        SELECT 'shipment_events', partner_event_id, 'source_clock_skew', 'warning',
               recorded_at, payload
        FROM src_shipment_events
        WHERE try_cast(recorded_at AS TIMESTAMPTZ) < try_cast(event_at AS TIMESTAMPTZ)
        UNION ALL
        SELECT 'shipments', shipment_id, 'invalid_declared_value', 'error',
               updated_at, declared_value_text
        FROM src_shipments
        WHERE try_cast(regexp_replace(declared_value_text, '[^0-9.-]', '', 'g') AS DECIMAL(18,2)) IS NULL
    """,
}

GOLD_QUERIES = {
    "daily_delivery_sla": """
        SELECT cast(s.created_at AS DATE) AS metric_date, s.destination_hub_id AS hub_id,
               s.service_level_code, count(*)::BIGINT AS shipment_count,
               count(*) FILTER (WHERE c.current_status = 'delivered')::BIGINT AS delivered_count,
               count(*) FILTER (
                   WHERE c.current_status = 'delivered' AND c.current_event_at <= s.promised_at
               )::BIGINT AS on_time_count,
               count(*) FILTER (
                   WHERE c.current_status = 'delivered' AND c.current_event_at > s.promised_at
               )::BIGINT AS late_count,
               avg(date_diff('minute', s.created_at, c.current_event_at))
                   FILTER (WHERE c.current_status = 'delivered')::DOUBLE AS avg_delivery_minutes
        FROM silver_fact_shipment s
        LEFT JOIN silver_shipment_current_state c USING (shipment_id)
        GROUP BY 1, 2, 3
    """,
    "hub_throughput": """
        SELECT cast(s.created_at AS DATE) AS metric_date, s.destination_hub_id AS hub_id,
               count(*)::BIGINT AS inbound_shipments,
               count(*) FILTER (WHERE c.current_status = 'delivered')::BIGINT AS delivered_shipments,
               count(*) FILTER (WHERE c.current_status NOT IN ('delivered','returned','cancelled'))::BIGINT AS backlog_shipments,
               count(*) FILTER (WHERE s.promised_at < current_timestamp AND c.current_status NOT IN ('delivered','returned','cancelled'))::BIGINT AS sla_at_risk
        FROM silver_fact_shipment s
        LEFT JOIN silver_shipment_current_state c USING (shipment_id)
        GROUP BY 1, 2
    """,
    "courier_productivity": """
        SELECT cast(attempted_at AS DATE) AS metric_date, courier_id,
               count(*)::BIGINT AS attempts,
               count(DISTINCT shipment_id)::BIGINT AS shipments,
               count(*) FILTER (WHERE outcome = 'delivered')::BIGINT AS successful_attempts,
               round(100.0 * count(*) FILTER (WHERE outcome = 'delivered') / nullif(count(*), 0), 2)::DOUBLE AS success_rate_pct
        FROM silver_fact_delivery_attempt
        WHERE courier_id IS NOT NULL
        GROUP BY 1, 2
    """,
    "merchant_delivery_scorecard": """
        SELECT s.merchant_id, count(*)::BIGINT AS shipments,
               count(*) FILTER (WHERE c.current_status = 'delivered')::BIGINT AS delivered,
               count(*) FILTER (WHERE c.current_status = 'returned')::BIGINT AS returned,
               count(*) FILTER (WHERE c.current_status = 'delivery_failed')::BIGINT AS failed,
               round(100.0 * count(*) FILTER (
                   WHERE c.current_status = 'delivered' AND c.current_event_at <= s.promised_at
               ) / nullif(count(*), 0), 2)::DOUBLE AS on_time_rate_pct
        FROM silver_fact_shipment s
        LEFT JOIN silver_shipment_current_state c USING (shipment_id)
        GROUP BY 1
    """,
    "delivery_failure_analysis": """
        SELECT cast(attempted_at AS DATE) AS metric_date,
               coalesce(reason_code, 'unknown') AS reason_code,
               count(*)::BIGINT AS failed_attempts,
               count(DISTINCT shipment_id)::BIGINT AS affected_shipments
        FROM silver_fact_delivery_attempt
        WHERE outcome = 'failed'
        GROUP BY 1, 2
    """,
    "route_efficiency": """
        SELECT route_date, hub_id, courier_id, count(*)::BIGINT AS planned_stops,
               count(*) FILTER (WHERE actual_arrival_at IS NOT NULL)::BIGINT AS visited_stops,
               avg(abs(actual_sequence - planned_sequence))::DOUBLE AS avg_sequence_deviation,
               avg(date_diff('minute', planned_arrival_at, actual_arrival_at))::DOUBLE AS avg_arrival_deviation_minutes
        FROM silver_fact_route_stop
        GROUP BY 1, 2, 3
    """,
    "delivery_margin_daily": """
        SELECT cast(c.charged_at AS DATE) AS metric_date, s.merchant_id,
               sum(c.amount_vnd)::DECIMAL(38,2) AS gross_charge_vnd,
               sum(CASE WHEN c.charge_status = 'voided' THEN c.amount_vnd ELSE 0 END)::DECIMAL(38,2) AS voided_vnd,
               sum(CASE WHEN c.charge_status = 'captured' THEN c.amount_vnd ELSE 0 END)::DECIMAL(38,2) AS net_charge_vnd
        FROM silver_fact_charge c JOIN silver_fact_shipment s USING (shipment_id)
        GROUP BY 1, 2
    """,
    "data_quality_daily": """
        SELECT try_cast(detected_at AS DATE) AS metric_date, rule_code, severity,
               count(*)::BIGINT AS rejected_records
        FROM silver_dq_rejected_records
        GROUP BY 1, 2, 3
    """,
}


def _string_value(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), sort_keys=True)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _extract_table(connection: psycopg.Connection, table: str) -> pa.Table:
    with connection.cursor() as cursor:
        cursor.execute(f'SELECT * FROM "{table}"')
        columns = [item.name for item in cursor.description]
        rows = [[_string_value(value) for value in row] for row in cursor.fetchall()]
    schema = pa.schema([pa.field(column, pa.string()) for column in columns])
    return pa.Table.from_pylist(
        [dict(zip(columns, row, strict=True)) for row in rows], schema=schema
    )


def extract_snapshot(settings: Settings) -> tuple[dict[str, pa.Table], dict[str, Any]]:
    with psycopg.connect(**settings.pg_kwargs()) as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        source_lsn, captured_at = connection.execute(
            "SELECT pg_current_wal_lsn()::text, transaction_timestamp()"
        ).fetchone()
        tables = {table: _extract_table(connection, table) for table in SOURCE_TABLES}
    return tables, {
        "captured_at": captured_at.isoformat(),
        "lsn": source_lsn,
        "rows": {table: data.num_rows for table, data in tables.items()},
    }


def _duckdb(settings: Settings) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("SET threads = 2")
    connection.execute(f"SET memory_limit = '{settings.batch_memory}'")
    connection.execute("SET TimeZone = 'UTC'")
    connection.execute("SET preserve_insertion_order = false")
    return connection


def build_silver(
    settings: Settings, source: dict[str, pa.Table]
) -> dict[str, pa.Table]:
    connection = _duckdb(settings)
    try:
        for name, data in source.items():
            connection.register(f"src_{name}", data)
        results: dict[str, pa.Table] = {}
        for name, query in SILVER_QUERIES.items():
            if name == "shipment_current_state":
                connection.register(
                    "silver_fact_shipment_event", results["fact_shipment_event"]
                )
            results[name] = connection.execute(query).to_arrow_table()
            connection.register(f"silver_{name}", results[name])
        return results
    finally:
        connection.close()


def build_gold(settings: Settings, silver: dict[str, pa.Table]) -> dict[str, pa.Table]:
    connection = _duckdb(settings)
    try:
        for name, data in silver.items():
            connection.register(f"silver_{name}", data)
        return {
            name: connection.execute(query).to_arrow_table()
            for name, query in GOLD_QUERIES.items()
        }
    finally:
        connection.close()


def _version(batch_id: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", batch_id.lower()).strip("_")
    return normalized[:80]


def namespaces_for(batch_id: str) -> dict[str, str]:
    version = _version(batch_id)
    return {
        "bronze": f"bronze_delivery_{version}",
        "silver": f"silver_delivery_{version}",
        "gold": f"gold_delivery_{version}",
    }


def table_properties(
    batch_id: str, layer: str, name: str, quality: str
) -> dict[str, str]:
    return {
        "kest.batch-id": batch_id,
        "kest.classification": classification_for(name),
        "kest.contract-version": "1.0.0",
        "kest.data-product": "delivery-operations",
        "kest.domain": "logistics",
        "kest.layer": layer,
        "kest.owner": "delivery-data@kest.local",
        "kest.quality-status": quality,
    }


def _read_tables(catalog, namespace: str, names) -> dict[str, pa.Table]:
    return {
        name: catalog.load_table((namespace, name)).scan().to_arrow() for name in names
    }


def _run_key(settings: Settings, batch_id: str, step: str) -> str:
    return f"{settings.iceberg_prefix.strip('/')}/_control/runs/{batch_id}/{step}.json"


def _load_json(settings: Settings, key: str) -> dict[str, Any]:
    response = s3_client(settings).get_object(Bucket=settings.s3_bucket, Key=key)
    return json.loads(response["Body"].read())


def _load_json_optional(settings: Settings, key: str) -> dict[str, Any] | None:
    try:
        return _load_json(settings, key)
    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 404:
            return None
        raise


def prepare_run(settings: Settings, batch_id: str) -> dict[str, Any]:
    """Capture one repeatable-read source snapshot and publish versioned Bronze."""
    state_key = _run_key(settings, batch_id, "prepare")
    existing = _load_json_optional(settings, state_key)
    if existing is not None:
        print(json.dumps(existing, indent=2, sort_keys=True))
        return existing
    namespaces = namespaces_for(batch_id)
    _, expected_etag = load_current_pointer(settings, required=False)
    catalog = iceberg_catalog(settings)
    try:
        source, source_metadata = extract_snapshot(settings)
        ensure_namespace(catalog, namespaces["bronze"])
        ensure_namespace(catalog, namespaces["silver"])
        ensure_namespace(catalog, namespaces["gold"])
        rows = {}
        for name, data in source.items():
            rows[name] = write_table(
                catalog,
                namespaces["bronze"],
                name,
                data,
                table_properties(batch_id, "bronze", name, "pending"),
            )
    except Exception:
        for namespace in reversed(namespaces.values()):
            remove_namespace(catalog, namespace)
        raise
    state = {
        "batch_id": batch_id,
        "captured_at": utc_now(),
        "expected_pointer_etag": expected_etag,
        "namespaces": namespaces,
        "rows": rows,
        "source_snapshot": source_metadata,
    }
    put_immutable_json(settings, state_key, state)
    print(json.dumps(state, indent=2, sort_keys=True))
    return state


def build_silver_job(settings: Settings, batch_id: str, name: str) -> int:
    if name not in SILVER_QUERIES:
        raise ValueError(f"Unknown Silver job: {name}")
    namespaces = namespaces_for(batch_id)
    catalog = iceberg_catalog(settings)
    source = _read_tables(catalog, namespaces["bronze"], SOURCE_TABLES)
    connection = _duckdb(settings)
    try:
        for source_name, data in source.items():
            connection.register(f"src_{source_name}", data)
        if name == "shipment_current_state":
            dependencies = _read_tables(
                catalog,
                namespaces["silver"],
                ("fact_shipment_event", "fact_shipment"),
            )
            for dependency, data in dependencies.items():
                connection.register(f"silver_{dependency}", data)
        data = connection.execute(SILVER_QUERIES[name]).to_arrow_table()
    finally:
        connection.close()
    rows = write_table(
        catalog,
        namespaces["silver"],
        name,
        data,
        table_properties(batch_id, "silver", name, "pending"),
    )
    print(f"Silver ready: {namespaces['silver']}.{name} ({rows} rows)")
    return rows


def build_gold_job(settings: Settings, batch_id: str, name: str) -> int:
    if name not in GOLD_QUERIES:
        raise ValueError(f"Unknown Gold job: {name}")
    namespaces = namespaces_for(batch_id)
    catalog = iceberg_catalog(settings)
    silver = _read_tables(catalog, namespaces["silver"], SILVER_QUERIES)
    connection = _duckdb(settings)
    try:
        for silver_name, data in silver.items():
            connection.register(f"silver_{silver_name}", data)
        data = connection.execute(GOLD_QUERIES[name]).to_arrow_table()
    finally:
        connection.close()
    rows = write_table(
        catalog,
        namespaces["gold"],
        name,
        data,
        table_properties(batch_id, "gold", name, "pending"),
    )
    print(f"Gold ready: {namespaces['gold']}.{name} ({rows} rows)")
    return rows


def run_quality_job(settings: Settings, batch_id: str) -> dict[str, Any]:
    namespaces = namespaces_for(batch_id)
    catalog = iceberg_catalog(settings)
    source = _read_tables(catalog, namespaces["bronze"], SOURCE_TABLES)
    silver = _read_tables(catalog, namespaces["silver"], SILVER_QUERIES)
    result = quality_result(batch_id, source, silver)
    if result["status"] != "passed":
        raise RuntimeError(f"DeliveryOps quality gate failed: {result['checks']}")
    put_immutable_json(settings, _run_key(settings, batch_id, "quality"), result)
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def publish_run(settings: Settings, batch_id: str) -> dict[str, Any]:
    """Verify every staged job and atomically advance the consumer pointer."""
    namespaces = namespaces_for(batch_id)
    pointer, _ = load_current_pointer(settings, required=False)
    if pointer and pointer.get("batch_id") == batch_id:
        print(json.dumps(pointer, indent=2, sort_keys=True))
        return pointer
    prepared = _load_json(settings, _run_key(settings, batch_id, "prepare"))
    quality = _load_json(settings, _run_key(settings, batch_id, "quality"))
    if quality["status"] != "passed":
        raise RuntimeError("Cannot publish a failed quality result")
    catalog = iceberg_catalog(settings)
    expected = {
        "bronze": SOURCE_TABLES,
        "silver": SILVER_QUERIES,
        "gold": GOLD_QUERIES,
    }
    row_counts: dict[str, dict[str, int]] = {}
    for layer, names in expected.items():
        actual = {table[-1] for table in catalog.list_tables((namespaces[layer],))}
        if actual != set(names):
            raise RuntimeError(
                f"Incomplete {layer} jobs: missing={sorted(set(names) - actual)}"
            )
        row_counts[layer] = {}
        for name in names:
            table = catalog.load_table((namespaces[layer], name))
            row_counts[layer][name] = sum(
                task.file.record_count for task in table.scan().plan_files()
            )

    completed_at = utc_now()
    manifest = {
        "batch_id": batch_id,
        "completed_at": completed_at,
        "contract_version": "1.0.0",
        "namespaces": namespaces,
        "quality_status": quality["status"],
        "rows": row_counts,
        "source_snapshot": prepared["source_snapshot"],
    }
    manifest_key = control_key(settings, "manifests", batch_id)
    quality_key = control_key(settings, "quality", batch_id)
    assets_key = control_key(settings, "assets", batch_id)
    lineage_key = control_key(settings, "openlineage", batch_id)
    put_immutable_json(settings, manifest_key, manifest)
    put_immutable_json(settings, quality_key, quality)
    put_immutable_json(
        settings, assets_key, asset_inventory(batch_id, namespaces, row_counts)
    )
    outputs = [
        f"{namespaces[layer]}.{name}"
        for layer, names in expected.items()
        for name in names
    ]
    put_immutable_json(
        settings,
        lineage_key,
        openlineage_event(batch_id, completed_at, list(SOURCE_TABLES), outputs),
    )
    result = {
        "batch_id": batch_id,
        "bronze_namespace": namespaces["bronze"],
        "silver_namespace": namespaces["silver"],
        "gold_namespace": namespaces["gold"],
        "manifest_key": manifest_key,
        "quality_key": quality_key,
        "assets_key": assets_key,
        "lineage_key": lineage_key,
    }
    publish_pointer(settings, result, prepared["expected_pointer_etag"])
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def quality_result(
    batch_id: str,
    source: dict[str, pa.Table],
    silver: dict[str, pa.Table],
) -> dict[str, Any]:
    events = source["shipment_events"].num_rows
    clean_events = silver["fact_shipment_event"].num_rows
    rejected = silver["dq_rejected_records"]
    connection = duckdb.connect()
    try:
        connection.register("dq", rejected)
        by_rule = {
            rule: count
            for rule, count in connection.execute(
                "SELECT rule_code, count(*)::BIGINT FROM dq GROUP BY rule_code"
            ).fetchall()
        }
        duplicate_rate = (events - clean_events) / max(events, 1)
        checks = {
            "duplicate_rate_below_2pct": duplicate_rate < 0.02,
            "orphan_rate_below_1pct": by_rule.get("orphan_shipment", 0) / max(events, 1)
            < 0.01,
            "shipment_count_preserved": (
                silver["fact_shipment"].num_rows == source["shipments"].num_rows
            ),
            "current_state_excludes_orphans": (
                silver["shipment_current_state"].num_rows
                == silver["fact_shipment"].num_rows
            ),
            "normalized_charge_amount_complete": True,
        }
        connection.register("charges", silver["fact_charge"])
        checks["normalized_charge_amount_complete"] = (
            connection.execute(
                "SELECT count(*) FROM charges WHERE amount_vnd IS NULL"
            ).fetchone()[0]
            == 0
        )
    finally:
        connection.close()
    return {
        "batch_id": batch_id,
        "checks": checks,
        "duplicate_rate": round(duplicate_rate, 6),
        "rejected_by_rule": by_rule,
        "rejected_rows": rejected.num_rows,
        "status": "passed" if all(checks.values()) else "failed",
    }


def run(settings: Settings) -> dict[str, Any]:
    batch_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )
    namespaces = namespaces_for(batch_id)
    _, expected_etag = load_current_pointer(settings, required=False)
    catalog = iceberg_catalog(settings)
    published = False
    try:
        source, source_metadata = extract_snapshot(settings)
        silver = build_silver(settings, source)
        gold = build_gold(settings, silver)
        quality = quality_result(batch_id, source, silver)
        if quality["status"] != "passed":
            raise RuntimeError(f"DeliveryOps quality gate failed: {quality['checks']}")

        layers = {"bronze": source, "silver": silver, "gold": gold}
        row_counts: dict[str, dict[str, int]] = {}
        for layer, tables in layers.items():
            namespace = namespaces[layer]
            ensure_namespace(catalog, namespace)
            row_counts[layer] = {}
            for name, data in tables.items():
                row_counts[layer][name] = write_table(
                    catalog,
                    namespace,
                    name,
                    data,
                    table_properties(batch_id, layer, name, quality["status"]),
                )

        completed_at = utc_now()
        manifest = {
            "batch_id": batch_id,
            "completed_at": completed_at,
            "contract_version": "1.0.0",
            "namespaces": namespaces,
            "quality_status": quality["status"],
            "rows": row_counts,
            "source_snapshot": source_metadata,
        }
        manifest_key = control_key(settings, "manifests", batch_id)
        quality_key = control_key(settings, "quality", batch_id)
        assets_key = control_key(settings, "assets", batch_id)
        lineage_key = control_key(settings, "openlineage", batch_id)
        put_immutable_json(settings, manifest_key, manifest)
        put_immutable_json(settings, quality_key, quality)
        put_immutable_json(
            settings, assets_key, asset_inventory(batch_id, namespaces, row_counts)
        )
        outputs = [
            f"{namespace}.{table}"
            for layer, namespace in namespaces.items()
            for table in row_counts[layer]
        ]
        put_immutable_json(
            settings,
            lineage_key,
            openlineage_event(batch_id, completed_at, list(SOURCE_TABLES), outputs),
        )
        pointer = {
            "assets_key": assets_key,
            "batch_id": batch_id,
            "gold_namespace": namespaces["gold"],
            "lineage_key": lineage_key,
            "manifest_key": manifest_key,
            "published_at": completed_at,
            "quality_key": quality_key,
            "schema_version": 1,
            "silver_namespace": namespaces["silver"],
        }
        publish_pointer(settings, pointer, expected_etag)
        published = True
    except Exception:
        if not published:
            for namespace in reversed(namespaces.values()):
                remove_namespace(catalog, namespace)
        raise

    result = {
        "batch_id": batch_id,
        "current_pointer": current_pointer_key(settings),
        "namespaces": namespaces,
        "quality": quality,
        "rows": row_counts,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return result
