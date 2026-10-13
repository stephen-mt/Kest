#!/usr/bin/env python3
"""Apply ordered, checksum-protected PostgreSQL and ClickHouse migrations."""

from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path

import clickhouse_connect
import psycopg
from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "infra" / "migrations"


def checksum(payload: str) -> str:
    return hashlib.sha256(payload.encode()).hexdigest()


def render(payload: str) -> str:
    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in os.environ:
            raise RuntimeError(f"Migration requires missing environment variable: {name}")
        return os.environ[name].replace("'", "''")

    return re.sub(r"\{\{([A-Z][A-Z0-9_]*)\}\}", replace, payload)


def ensure_postgres_databases() -> None:
    databases = {
        "business_source": "BUSINESS_SOURCE_PASSWORD",
        "platform_control": "PLATFORM_CONTROL_PASSWORD",
        "airflow": "AIRFLOW_DB_PASSWORD",
        "superset": "SUPERSET_DB_PASSWORD",
        "openmetadata": "OPENMETADATA_DB_PASSWORD",
        "keycloak": "KEYCLOAK_DB_PASSWORD",
    }
    dsn = (
        f"postgresql://postgres:{os.environ['POSTGRES_ADMIN_PASSWORD']}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/postgres"
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        for name, password_key in databases.items():
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (name,))
                if cur.fetchone():
                    cur.execute(
                        sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD {}").format(
                            sql.Identifier(name),
                            sql.Literal(os.environ[password_key]),
                        )
                    )
                else:
                    cur.execute(
                        sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                            sql.Identifier(name),
                            sql.Literal(os.environ[password_key]),
                        )
                    )
                cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (name,))
                if not cur.fetchone():
                    cur.execute(
                        sql.SQL("CREATE DATABASE {} OWNER {}").format(
                            sql.Identifier(name), sql.Identifier(name)
                        )
                    )


def migrate_postgres() -> None:
    dsn = (
        f"postgresql://platform_control:{os.environ['PLATFORM_CONTROL_PASSWORD']}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/platform_control"
    )
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS platform_schema_migration (
              version text PRIMARY KEY,
              checksum text NOT NULL,
              applied_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('kest:postgres:migrations'))")
        for path in sorted((MIGRATIONS / "postgres").glob("*.sql")):
            version = path.stem
            payload = render(path.read_text())
            digest = checksum(payload)
            cur.execute(
                "SELECT checksum FROM platform_schema_migration WHERE version=%s", (version,)
            )
            row = cur.fetchone()
            if row:
                if row[0] != digest:
                    raise RuntimeError(f"Applied PostgreSQL migration changed: {version}")
                continue
            cur.execute(payload)
            cur.execute(
                "INSERT INTO platform_schema_migration(version, checksum) VALUES (%s,%s)",
                (version, digest),
            )
            print(f"postgres migration applied: {version}")


def migrate_clickhouse() -> None:
    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username="default",
        password=os.environ["CLICKHOUSE_ADMIN_PASSWORD"],
    )
    client.command("CREATE DATABASE IF NOT EXISTS platform ENGINE=Atomic")
    client.command(
        """
        CREATE TABLE IF NOT EXISTS platform.schema_migration (
          version String,
          checksum String,
          applied_at DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE=ReplacingMergeTree(applied_at) ORDER BY version
        """
    )
    for path in sorted((MIGRATIONS / "clickhouse").glob("*.sql")):
        version = path.stem
        payload = render(path.read_text())
        digest = checksum(payload)
        rows = client.query(
            "SELECT checksum FROM platform.schema_migration FINAL WHERE version=%(version)s",
            parameters={"version": version},
        ).result_rows
        if rows:
            if rows[0][0] != digest:
                raise RuntimeError(f"Applied ClickHouse migration changed: {version}")
            continue
        for statement in (item.strip() for item in payload.split(";")):
            if statement and not statement.startswith("--"):
                client.command(statement)
            elif statement and "\n" in statement:
                body = "\n".join(
                    line for line in statement.splitlines() if not line.lstrip().startswith("--")
                ).strip()
                if body:
                    client.command(body)
        client.insert(
            "platform.schema_migration",
            [[version, digest]],
            column_names=["version", "checksum"],
        )
        print(f"clickhouse migration applied: {version}")


def main() -> None:
    ensure_postgres_databases()
    migrate_postgres()
    migrate_clickhouse()


if __name__ == "__main__":
    main()
