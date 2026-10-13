#!/usr/bin/env bash
set -euo pipefail

create_role_and_db() {
  local role="$1" password="$2" database="$3"
  psql --username "$POSTGRES_USER" --dbname postgres \
    --set=role_name="$role" --set=role_password="$password" --set=db_name="$database" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'role_name', :'role_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'role_name')\gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'db_name', :'role_name')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = :'db_name')\gexec
SQL
}

create_role_and_db business_source "$BUSINESS_SOURCE_PASSWORD" business_source
create_role_and_db platform_control "$PLATFORM_CONTROL_PASSWORD" platform_control
create_role_and_db airflow "$AIRFLOW_DB_PASSWORD" airflow
create_role_and_db superset "$SUPERSET_DB_PASSWORD" superset
create_role_and_db openmetadata "$OPENMETADATA_DB_PASSWORD" openmetadata
create_role_and_db keycloak "$KEYCLOAK_DB_PASSWORD" keycloak

psql --username "$POSTGRES_USER" --dbname platform_control \
  --set=control_password="$PLATFORM_CONTROL_PASSWORD" <<'SQL'
CREATE TABLE IF NOT EXISTS batch_manifest (
    batch_id uuid PRIMARY KEY,
    source text NOT NULL,
    object_prefix text NOT NULL,
    checksum text NOT NULL,
    row_count bigint NOT NULL CHECK (row_count >= 0),
    watermark jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL CHECK (status IN (
      'STORED', 'LOADING', 'LOADED', 'FAILED', 'QUARANTINED'
    )),
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS batch_manifest_source_status_idx
    ON batch_manifest(source, status, created_at);

CREATE TABLE IF NOT EXISTS batch_objects (
    batch_id uuid NOT NULL REFERENCES batch_manifest(batch_id) ON DELETE RESTRICT,
    table_name text NOT NULL,
    object_path text NOT NULL,
    checksum text NOT NULL,
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    row_count bigint NOT NULL CHECK (row_count >= 0),
    PRIMARY KEY (batch_id, object_path)
);

CREATE TABLE IF NOT EXISTS source_checkpoint (
    source text PRIMARY KEY,
    watermark jsonb NOT NULL,
    batch_id uuid REFERENCES batch_manifest(batch_id),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id uuid PRIMARY KEY,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status text NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    error text
);

CREATE TABLE IF NOT EXISTS publication_manifest (
    publication_id uuid PRIMARY KEY,
    run_id uuid REFERENCES pipeline_run(run_id),
    dataset text NOT NULL,
    candidate_checksum text NOT NULL,
    previous_checksum text,
    published_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, dataset)
);

CREATE TABLE IF NOT EXISTS backup_manifest (
    backup_id uuid PRIMARY KEY,
    path text NOT NULL,
    checksum text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    verified_at timestamptz
);

ALTER TABLE batch_manifest OWNER TO platform_control;
ALTER TABLE batch_objects OWNER TO platform_control;
ALTER TABLE source_checkpoint OWNER TO platform_control;
ALTER TABLE pipeline_run OWNER TO platform_control;
ALTER TABLE publication_manifest OWNER TO platform_control;
ALTER TABLE backup_manifest OWNER TO platform_control;
SQL
