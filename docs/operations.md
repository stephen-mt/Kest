# Operator runbook

This file is intentionally limited to platform operations. User-facing steps
live in the [DE cookbook](de-cookbook.md) and [DA cookbook](da-cookbook.md).

## Start and inspect

```bash
make up
make credentials
make health
```

`make up` is idempotent: it generates missing local secrets, validates and
starts pinned services, applies database/object-store bootstrap, creates
Airflow pools/users and provisions Superset assets. It does not load sample
data. `make demo` is the explicit acceptance fixture.

`make catalog` starts OpenMetadata, synchronizes assets and unpauses its daily
metadata DAG. `make catalog-down` pauses that DAG before stopping the catalog,
so an optional service cannot generate routine Airflow failures.

## Failed pipeline

1. Run `make health`; distinguish dependency failure from bad source data.
2. Inspect the failed Airflow task and its batch ID. Records are never put in
   XCom.
3. For timeout/429/transient failures, retry the task. Checkpoints do not move
   on incomplete extracts and completed batch loads are no-ops.
4. For contract failure, quarantine/fix the source or update the reviewed
   contract. Do not bypass dbt tests or publish an untested candidate.
5. Use `controlled_backfill` with `dry_run=true` before a dated rebuild. The
   one-slot pool keeps it from starving analyst queries.

## Publication incident

A candidate test failure must leave Gold and its publication ID unchanged. Two
publishers are serialized by an advisory lock and `gold_publish=1`. Restore a
retained mart snapshot only with `publish_service`, then record the affected
dataset, publication ID, reason and validation query.

## Backup and recovery

```bash
make backup
# copy the printed bundle to independent storage
make restore-test
```

Backup covers the five PostgreSQL databases, raw S3 objects and ClickHouse raw
and Gold. Restore verification uses isolated PostgreSQL databases, a separate
bucket and the `clickhouse-restore` service, then compares counts/checksums. The
commands are host-side Python tools because an Airflow worker is intentionally
not given the Docker socket. Schedule the host command with the organization's
backup runner and alert on age/failure.

## Upgrade

Update one pinned component at a time. Change both the readable tag and digest,
rebuild immutable images, deploy to staging, run the full acceptance suite and
record the evidence. Floating `latest` tags are not a valid production upgrade
strategy. Stop and take a verified backup before stateful format migrations.
