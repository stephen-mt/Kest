from __future__ import annotations

import json
import os
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


def _event(settings: Settings, publication_id: str, event_type: str, details: dict | None = None) -> None:
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO publication_event(event_id,publication_id,event_type,actor,details) "
            "VALUES (%s,%s,%s,%s,%s)",
            (
                str(uuid.uuid4()),
                publication_id,
                event_type,
                os.getenv("KEST_REQUESTED_BY", "system"),
                json.dumps(details or {}),
            ),
        )


def ensure_release(settings: Settings, run_id: str) -> str:
    release_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"kest:release:{run_id}"))
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dataset_release(
              release_id,run_id,status,requested_by,environment,git_sha,image_ref
            )
            SELECT %s,run_id,'PREPARING',requested_by,environment,git_sha,image_ref
            FROM pipeline_run WHERE run_id=%s
            ON CONFLICT (release_id) DO NOTHING
            """,
            (release_id, run_id),
        )
    return release_id


def publish_mart(settings: Settings, table: str, run_id: str) -> str:
    if table not in MARTS:
        raise ValueError(f"Unknown mart: {table}")
    client = clickhouse_client(settings, "publisher")
    publication_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"kest:publication:{run_id}:{table}"))
    with control_connection(settings, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (f"publish:{table}",))
        if not cur.fetchone()[0]:
            raise RuntimeError(f"Another publisher owns the lock for {table}")
        mutable_manifest = False
        try:
            release_id = ensure_release(settings, run_id)
            candidate_exists = client.query(
                "SELECT count() FROM system.tables "
                "WHERE database='gold_candidate' AND name=%(table)s",
                parameters={"table": table},
            ).first_row[0]
            candidate_checksum = (
                table_checksum(client, "gold_candidate", table) if candidate_exists else None
            )
            production_exists = client.query(
                "SELECT count() FROM system.tables WHERE database='gold' AND name=%(table)s",
                parameters={"table": table},
            ).first_row[0]
            production_checksum = (
                table_checksum(client, "gold", table) if production_exists else None
            )

            with control_connection(settings) as manifest_conn, manifest_conn.cursor() as manifest_cur:
                manifest_cur.execute(
                    """
                    SELECT candidate_checksum,status,previous_checksum,production_checksum,snapshot_table
                    FROM publication_manifest WHERE publication_id=%s FOR UPDATE
                    """,
                    (publication_id,),
                )
                manifest = manifest_cur.fetchone()
                if manifest:
                    expected_checksum, status, previous_checksum, recorded_production, snapshot = manifest
                    if status == "COMMITTED":
                        if production_checksum != recorded_production:
                            raise RuntimeError(
                                f"Committed publication drift for {table}: "
                                f"gold={production_checksum} recorded={recorded_production}"
                            )
                        return publication_id
                    mutable_manifest = True
                    if production_checksum == expected_checksum:
                        manifest_cur.execute(
                            """
                            UPDATE publication_manifest
                            SET status='COMMITTED', production_checksum=%s,
                                committed_at=coalesce(committed_at, now()), error=NULL
                            WHERE publication_id=%s
                            """,
                            (expected_checksum, publication_id),
                        )
                    elif candidate_checksum != expected_checksum:
                        raise RuntimeError(
                            f"Prepared candidate changed for {table}: "
                            f"current={candidate_checksum} expected={expected_checksum}"
                        )
                    else:
                        manifest_cur.execute(
                            "UPDATE publication_manifest SET status='PREPARED', error=NULL "
                            "WHERE publication_id=%s",
                            (publication_id,),
                        )
                else:
                    if candidate_checksum is None:
                        raise RuntimeError(f"Candidate does not exist: gold_candidate.{table}")
                    if candidate_checksum.startswith("0:"):
                        raise RuntimeError(f"Refusing to publish empty candidate: {table}")
                    expected_checksum = candidate_checksum
                    previous_checksum = None
                    snapshot = None
                    manifest_cur.execute(
                        """
                        INSERT INTO publication_manifest(
                          publication_id,run_id,release_id,dataset,candidate_checksum,status,requested_by
                        ) VALUES (%s,%s,%s,%s,%s,'PREPARED',%s)
                        """,
                        (
                            publication_id,
                            run_id,
                            release_id,
                            table,
                            expected_checksum,
                            os.getenv("KEST_REQUESTED_BY", "system"),
                        ),
                    )
                    mutable_manifest = True

            if production_checksum == expected_checksum:
                _event(
                    settings,
                    publication_id,
                    "RECONCILED",
                    {"reason": "Gold already matches the prepared checksum"},
                )
                return publication_id

            _event(settings, publication_id, "PREPARED", {"candidate_checksum": expected_checksum})
            if production_exists:
                if previous_checksum and previous_checksum != production_checksum:
                    raise RuntimeError(
                        f"Gold changed while publication was prepared for {table}: "
                        f"current={production_checksum} expected_previous={previous_checksum}"
                    )
                previous_checksum = production_checksum
                snapshot = snapshot or f"{table}__{publication_id.replace('-', '')[:12]}"
                snapshot_exists = client.query(
                    "SELECT count() FROM system.tables WHERE database='gold_history' AND name=%(name)s",
                    parameters={"name": snapshot},
                ).first_row[0]
                if not snapshot_exists:
                    client.command(f"CREATE TABLE gold_history.`{snapshot}` AS gold.{table}")
                    client.command(f"INSERT INTO gold_history.`{snapshot}` SELECT * FROM gold.{table}")
                snapshot_checksum = table_checksum(client, "gold_history", snapshot)
                if snapshot_checksum != previous_checksum:
                    raise RuntimeError(
                        f"Rollback snapshot mismatch for {table}: "
                        f"snapshot={snapshot_checksum} gold={previous_checksum}"
                    )
                with control_connection(settings) as manifest_conn, manifest_conn.cursor() as manifest_cur:
                    manifest_cur.execute(
                        "UPDATE publication_manifest SET previous_checksum=%s, snapshot_table=%s "
                        "WHERE publication_id=%s",
                        (previous_checksum, snapshot, publication_id),
                    )
            else:
                client.command(f"CREATE TABLE gold.{table} AS gold_candidate.{table}")
            client.command(f"EXCHANGE TABLES gold_candidate.{table} AND gold.{table}")
            production_checksum = table_checksum(client, "gold", table)
            if production_checksum != expected_checksum:
                raise RuntimeError(
                    f"Post-publish checksum mismatch for {table}: "
                    f"{production_checksum}/{expected_checksum}"
                )
            with control_connection(settings) as manifest_conn, manifest_conn.cursor() as manifest_cur:
                manifest_cur.execute(
                    """
                    UPDATE publication_manifest
                    SET status='COMMITTED', previous_checksum=%s, production_checksum=%s,
                        snapshot_table=%s, committed_at=now(), error=NULL
                    WHERE publication_id=%s
                    """,
                    (previous_checksum, production_checksum, snapshot, publication_id),
                )
            _event(settings, publication_id, "COMMITTED", {"production_checksum": production_checksum})
            _retain_three_snapshots(client, table)
            return publication_id
        except Exception as error:
            if mutable_manifest:
                with control_connection(settings) as manifest_conn, manifest_conn.cursor() as manifest_cur:
                    manifest_cur.execute(
                        "UPDATE publication_manifest SET status='FAILED', error=%s "
                        "WHERE publication_id=%s AND status<>'COMMITTED'",
                        (str(error)[:4000], publication_id),
                    )
                _event(settings, publication_id, "FAILED", {"error": str(error)[:4000]})
            raise
        finally:
            cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (f"publish:{table}",))


def publish_all(settings: Settings, run_id: str) -> dict[str, str]:
    release_id = ensure_release(settings, run_id)
    publications: dict[str, str] = {}
    try:
        for mart in MARTS:
            publications[mart] = publish_mart(settings, mart, run_id)
        with control_connection(settings) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE dataset_release SET status='COMMITTED', committed_at=now(), error=NULL WHERE release_id=%s",
                (release_id,),
            )
        return publications
    except Exception as error:
        with control_connection(settings) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE dataset_release SET status=%s, error=%s WHERE release_id=%s",
                ("PARTIAL" if publications else "FAILED", str(error)[:4000], release_id),
            )
        raise


def rollback_mart(
    settings: Settings,
    table: str,
    publication_id: str,
    reason: str,
    *,
    dry_run: bool = True,
) -> dict[str, str | bool]:
    if table not in MARTS:
        raise ValueError(f"Unknown mart: {table}")
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status,snapshot_table,previous_checksum
            FROM publication_manifest WHERE publication_id=%s AND dataset=%s
            """,
            (publication_id, table),
        )
        row = cur.fetchone()
    if not row or row[0] != "COMMITTED" or not row[1]:
        raise ValueError("Publication is not a committed release with a rollback snapshot")
    snapshot, target_checksum = row[1], row[2]
    client = clickhouse_client(settings, "publisher")
    current_checksum = table_checksum(client, "gold", table)
    plan: dict[str, str | bool] = {
        "dry_run": dry_run,
        "dataset": table,
        "publication_id": publication_id,
        "current_checksum": current_checksum,
        "target_checksum": target_checksum,
        "snapshot": snapshot,
        "reason": reason,
    }
    if dry_run:
        return plan

    from kest_ingestion.workflows import finish_run, start_run

    run_id = start_run(settings, run_type="ROLLBACK", reason=reason, source=table)
    release_id = ensure_release(settings, run_id)
    rollback_id = str(uuid.uuid4())
    with control_connection(settings, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (f"publish:{table}",))
        if not cur.fetchone()[0]:
            finish_run(settings, run_id, "FAILED", f"Another publisher owns the lock for {table}")
            raise RuntimeError(f"Another publisher owns the lock for {table}")
        try:
            client.command(f"EXCHANGE TABLES gold_history.`{snapshot}` AND gold.{table}")
            actual = table_checksum(client, "gold", table)
            if actual != target_checksum:
                client.command(f"EXCHANGE TABLES gold_history.`{snapshot}` AND gold.{table}")
                raise RuntimeError(f"Rollback checksum mismatch for {table}: {actual}/{target_checksum}")
            with control_connection(settings) as manifest_conn, manifest_conn.cursor() as manifest_cur:
                manifest_cur.execute(
                    """
                    INSERT INTO publication_manifest(
                      publication_id,run_id,release_id,dataset,candidate_checksum,previous_checksum,
                      production_checksum,snapshot_table,status,requested_by,committed_at,rollback_of
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'COMMITTED',%s,now(),%s)
                    """,
                    (
                        rollback_id, run_id, release_id, table, target_checksum, current_checksum,
                        actual, snapshot, os.getenv("KEST_REQUESTED_BY", "system"), publication_id,
                    ),
                )
                manifest_cur.execute(
                    "UPDATE publication_manifest SET status='ROLLED_BACK', rolled_back_at=now() WHERE publication_id=%s",
                    (publication_id,),
                )
                manifest_cur.execute(
                    "UPDATE dataset_release SET status='COMMITTED', committed_at=now() WHERE release_id=%s",
                    (release_id,),
                )
            _event(settings, publication_id, "ROLLED_BACK", {"rollback_publication_id": rollback_id})
            _event(settings, rollback_id, "COMMITTED", {"rollback_of": publication_id})
            finish_run(settings, run_id, "SUCCESS")
            return {**plan, "dry_run": False, "rollback_publication_id": rollback_id}
        except Exception as error:
            finish_run(settings, run_id, "FAILED", str(error)[:4000])
            raise
        finally:
            cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (f"publish:{table}",))
