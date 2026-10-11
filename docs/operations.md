# Operations runbook

## Normal operation

- `make health`: PostgreSQL control DB, ClickHouse and SILO S3.
- `make pipeline`: synchronous source-to-Gold run for debugging.
- Airflow DAG `ecommerce_end_to_end`: manual until an operator chooses a schedule.
- `make failure-test`: safe replay, contract, lock and RBAC scenarios.

## Backfill and replay

Trigger `controlled_backfill` with source, inclusive start/end dates, reason and
`dry_run=true` first. Empty, reversed, future and over-366-day windows are
rejected. The heavy-backfill pool has one slot and cannot consume all analyst
resources. Raw batch replay is idempotent by `batch_id`; a completed batch is a
no-op.

## Publish incident

Candidate tests run before publishing. A failed test leaves Gold and its
publication ID unchanged. Publication takes a PostgreSQL advisory lock and
Airflow's one-slot `gold_publish` pool. Gold history retains three snapshots per
mart. Restore a snapshot only with the `publish_service` identity and record the
incident/release externally.

## Recovery

Run `make backup`, copy the printed directory off-host, then `make restore-test`.
The latter restores PostgreSQL into isolated verification databases, restores
raw objects into a separate bucket, and restores ClickHouse into the dedicated
`clickhouse-restore` service before comparing deterministic checksums. Never
treat an on-host backup as host-loss disaster recovery.

## Capacity boundary

If ingestion, transformation and dashboard workloads can no longer meet the
documented latency while resource profiles are enforced, scale by separating
compute/storage and introducing replicated/distributed ClickHouse. Do not
present this Compose deployment as HA or unlimited scale.
