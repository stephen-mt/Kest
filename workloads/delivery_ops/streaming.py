"""RisingWave CDC source and operational materialized views."""

from __future__ import annotations

import json
import time
from typing import Any

import psycopg
from psycopg import sql

from workloads.delivery_ops.config import Settings
from workloads.delivery_ops.source import release_replication_slot

SOURCE = "delivery_postgres_cdc"
TABLES = {
    "delivery_shipments": "shipments",
    "delivery_shipment_events": "shipment_events",
    "delivery_attempts": "delivery_attempts",
}
MATERIALIZED_VIEWS = {
    "delivery_live_hub_backlog": """
        SELECT destination_hub_id AS hub_id, service_level_code,
               count(*)::BIGINT AS shipment_count,
               count(*) FILTER (
                   WHERE current_status NOT IN ('delivered', 'returned', 'cancelled')
               )::BIGINT AS open_shipments,
               count(*) FILTER (
                   WHERE current_status = 'delivery_failed'
               )::BIGINT AS failed_shipments
        FROM delivery_shipments
        GROUP BY destination_hub_id, service_level_code
    """,
    "delivery_live_merchant_funnel": """
        SELECT merchant_id, current_status,
               count(*)::BIGINT AS shipment_count,
               min(created_at) AS oldest_created_at,
               max(updated_at) AS last_source_update_at
        FROM delivery_shipments
        GROUP BY merchant_id, current_status
    """,
    "delivery_live_event_volume": """
        SELECT hub_id, event_type,
               count(*)::BIGINT AS event_count,
               max(recorded_at) AS latest_recorded_at
        FROM delivery_shipment_events
        GROUP BY hub_id, event_type
    """,
    "delivery_live_failure_reasons": """
        SELECT coalesce(reason_code_raw, 'unknown') AS reason_code,
               count(*)::BIGINT AS failed_attempts,
               count(DISTINCT shipment_id)::BIGINT AS affected_shipments,
               max(recorded_at) AS latest_recorded_at
        FROM delivery_attempts
        WHERE outcome = 'failed'
        GROUP BY coalesce(reason_code_raw, 'unknown')
    """,
    "delivery_live_sla_watchlist": """
        SELECT shipment_id, tracking_number, merchant_id,
               destination_hub_id AS hub_id, service_level_code,
               current_status, promised_at, updated_at
        FROM delivery_shipments
        WHERE current_status NOT IN ('delivered', 'returned', 'cancelled')
    """,
}


def _literal(value: str) -> sql.Literal:
    return sql.Literal(value)


def setup(settings: Settings) -> None:
    """Create an isolated PostgreSQL CDC source and live views."""
    with psycopg.connect(**settings.risingwave_kwargs(), autocommit=True) as connection:
        exists = connection.execute(
            "SELECT count(*) FROM rw_catalog.rw_sources WHERE name = %s", (SOURCE,)
        ).fetchone()[0]
        if exists:
            print("RisingWave DeliveryOps source already exists; preserved")
            return
        connection.execute(
            sql.SQL(
                """
                CREATE SOURCE {source} WITH (
                    connector = 'postgres-cdc',
                    hostname = {host},
                    port = {port},
                    username = {user},
                    password = {password},
                    database.name = {database},
                    schema.name = 'public',
                    slot.name = 'kest_delivery_risingwave',
                    publication.name = 'kest_delivery_risingwave'
                )
                """
            ).format(
                source=sql.Identifier(SOURCE),
                host=_literal(settings.pg_host),
                port=_literal(str(settings.pg_port)),
                user=_literal(settings.pg_user),
                password=_literal(settings.pg_password),
                database=_literal(settings.pg_database),
            )
        )
        for local_name, source_name in TABLES.items():
            connection.execute(
                sql.SQL("CREATE TABLE {} (*) FROM {} TABLE {}").format(
                    sql.Identifier(local_name),
                    sql.Identifier(SOURCE),
                    _literal(f"public.{source_name}"),
                )
            )
        for name, query in MATERIALIZED_VIEWS.items():
            connection.execute(
                sql.SQL("CREATE MATERIALIZED VIEW {} AS ").format(sql.Identifier(name))
                + sql.SQL(query)
            )
    print(
        f"RisingWave ready: {len(TABLES)} CDC tables, "
        f"{len(MATERIALIZED_VIEWS)} materialized views"
    )


def check(settings: Settings) -> dict[str, Any]:
    with psycopg.connect(**settings.risingwave_kwargs()) as connection:
        rows = {
            name: connection.execute(
                sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(name))
            ).fetchone()[0]
            for name in (*TABLES, *MATERIALIZED_VIEWS)
        }
    result = {"cdc_source": SOURCE, "rows": rows}
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def teardown(settings: Settings) -> None:
    """Release the replication slot so an inactive lab cannot retain WAL."""
    with psycopg.connect(**settings.risingwave_kwargs(), autocommit=True) as connection:
        for name in reversed(MATERIALIZED_VIEWS):
            connection.execute(
                sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {} CASCADE").format(
                    sql.Identifier(name)
                )
            )
        for name in reversed(TABLES):
            connection.execute(
                sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(name))
            )
        connection.execute(
            sql.SQL("DROP SOURCE IF EXISTS {} CASCADE").format(sql.Identifier(SOURCE))
        )
    for _ in range(60):
        with psycopg.connect(**settings.pg_kwargs()) as source:
            active = source.execute(
                "SELECT active FROM pg_replication_slots WHERE slot_name = %s",
                ("kest_delivery_risingwave",),
            ).fetchone()
        if active is None or not active[0]:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("RisingWave replication slot did not become inactive")
    release_replication_slot(settings, "kest_delivery_risingwave")
    print("RisingWave DeliveryOps source removed")
