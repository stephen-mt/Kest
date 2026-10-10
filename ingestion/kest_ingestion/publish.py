from __future__ import annotations

import uuid

from kest_ingestion.config import Settings
from kest_ingestion.db import clickhouse_client, control_connection

MARTS = ("mart_daily_revenue", "mart_conversion_funnel", "mart_customer_orders")


def table_checksum(client, database: str, table: str) -> str:
    row = client.query(
        f"SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM {database}.{table}"
    ).first_row
    return f"{row[0]}:{row[1] or 0}"


def _retain_three_snapshots(client, table: str) -> None:
    rows = client.query(
        "SELECT name FROM system.tables WHERE database='gold_history' AND name LIKE %(prefix)s ORDER BY metadata_modification_time DESC",
        parameters={"prefix": f"{table}__%"},
    ).result_rows
    for (name,) in rows[3:]:
        client.command(f"DROP TABLE IF EXISTS gold_history.`{name}`")


def publish_mart(settings: Settings, table: str, run_id: str) -> str:
    if table not in MARTS:
        raise ValueError(f"Unknown mart: {table}")
    client = clickhouse_client(settings, "publisher")
    candidate_exists = client.query(
        "SELECT count() FROM system.tables WHERE database='gold_candidate' AND name=%(table)s",
        parameters={"table": table},
    ).first_row[0]
    if not candidate_exists:
        raise RuntimeError(f"Candidate does not exist: gold_candidate.{table}")
    candidate_checksum = table_checksum(client, "gold_candidate", table)
    if candidate_checksum.startswith("0:"):
        raise RuntimeError(f"Refusing to publish empty candidate: {table}")

    with control_connection(settings, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (f"publish:{table}",))
        if not cur.fetchone()[0]:
            raise RuntimeError(f"Another publisher owns the lock for {table}")
        try:
            production_exists = client.query(
                "SELECT count() FROM system.tables WHERE database='gold' AND name=%(table)s",
                parameters={"table": table},
            ).first_row[0]
            previous_checksum = None
            if production_exists:
                previous_checksum = table_checksum(client, "gold", table)
                snapshot = f"{table}__{run_id.replace('-', '')[:12]}"
                client.command(f"CREATE TABLE gold_history.`{snapshot}` AS gold.{table}")
                client.command(f"INSERT INTO gold_history.`{snapshot}` SELECT * FROM gold.{table}")
            else:
                client.command(f"CREATE TABLE gold.{table} AS gold_candidate.{table}")
            client.command(f"EXCHANGE TABLES gold_candidate.{table} AND gold.{table}")
            publication_id = str(uuid.uuid4())
            with control_connection(settings) as manifest_conn, manifest_conn.cursor() as manifest_cur:
                manifest_cur.execute(
                    "INSERT INTO publication_manifest(publication_id, run_id, dataset, candidate_checksum, previous_checksum) VALUES (%s,%s,%s,%s,%s)",
                    (publication_id, run_id, table, candidate_checksum, previous_checksum),
                )
            _retain_three_snapshots(client, table)
            return publication_id
        finally:
            cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (f"publish:{table}",))


def publish_all(settings: Settings, run_id: str) -> dict[str, str]:
    return {mart: publish_mart(settings, mart, run_id) for mart in MARTS}
