#!/usr/bin/env bash
set -euo pipefail

clickhouse-client --multiquery --query "
CREATE DATABASE IF NOT EXISTS raw ENGINE = Atomic;
CREATE DATABASE IF NOT EXISTS raw_staging ENGINE = Atomic;
CREATE DATABASE IF NOT EXISTS staging ENGINE = Atomic;
CREATE DATABASE IF NOT EXISTS core ENGINE = Atomic;
CREATE DATABASE IF NOT EXISTS gold_candidate ENGINE = Atomic;
CREATE DATABASE IF NOT EXISTS gold ENGINE = Atomic;
CREATE DATABASE IF NOT EXISTS gold_history ENGINE = Atomic;
CREATE DATABASE IF NOT EXISTS platform ENGINE = Atomic;

CREATE ROLE IF NOT EXISTS de_development;
CREATE ROLE IF NOT EXISTS da_readonly;
CREATE ROLE IF NOT EXISTS pipeline_service;
CREATE ROLE IF NOT EXISTS publish_service;
CREATE ROLE IF NOT EXISTS platform_admin;

CREATE SETTINGS PROFILE IF NOT EXISTS analyst_limits SETTINGS
  readonly = 1,
  max_threads = 2,
  max_memory_usage = 1000000000,
  max_execution_time = 30,
  max_rows_to_read = 10000000;

CREATE SETTINGS PROFILE IF NOT EXISTS development_limits SETTINGS
  max_threads = 4,
  max_memory_usage = 2000000000,
  max_execution_time = 120,
  max_rows_to_read = 50000000;

CREATE USER IF NOT EXISTS pipeline IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_PIPELINE_PASSWORD}';
CREATE USER IF NOT EXISTS publisher IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_PUBLISH_PASSWORD}';
CREATE USER IF NOT EXISTS da IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_DA_PASSWORD}';
CREATE USER IF NOT EXISTS superset_ro IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_DA_PASSWORD}';
CREATE USER IF NOT EXISTS metadata_reader IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_METADATA_PASSWORD}';

GRANT pipeline_service TO pipeline;
GRANT publish_service TO publisher;
GRANT da_readonly TO da;
GRANT da_readonly TO superset_ro;
ALTER USER da SETTINGS PROFILE analyst_limits;
ALTER USER superset_ro SETTINGS PROFILE analyst_limits;

GRANT SELECT ON raw.* TO de_development;
GRANT SELECT ON staging.* TO de_development;
GRANT SELECT ON core.* TO de_development;
GRANT SELECT ON gold.* TO de_development;
GRANT SELECT ON gold.* TO da_readonly;

GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER, TRUNCATE ON raw.* TO pipeline_service;
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER, TRUNCATE ON raw_staging.* TO pipeline_service;
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER, TRUNCATE ON staging.* TO pipeline_service;
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER, TRUNCATE ON core.* TO pipeline_service;
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER, TRUNCATE ON gold_candidate.* TO pipeline_service;
GRANT SELECT ON raw.* TO pipeline_service;
GRANT SELECT ON gold_candidate.* TO publish_service;
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER, TRUNCATE ON gold.* TO publish_service;
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER, TRUNCATE ON gold_candidate.* TO publish_service;
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE, ALTER, TRUNCATE ON gold_history.* TO publish_service;
GRANT SELECT, SHOW ON system.* TO metadata_reader;
GRANT SELECT ON raw.* TO metadata_reader;
GRANT SELECT ON staging.* TO metadata_reader;
GRANT SELECT ON core.* TO metadata_reader;
GRANT SELECT ON gold.* TO metadata_reader;
"

clickhouse-client --multiquery < /docker-entrypoint-initdb.d/10-raw-tables.sql
