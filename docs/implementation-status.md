# Implementation status

Verified at `2026-10-11T07:16:59Z` against Git commit `c531c2a` on
`codex/production-oss-platform`.

This file records commands that were executed against this checkout. A healthy
container alone is not treated as proof that a data or metadata workflow works.

| Gate | Executed command or run | Evidence |
|---|---|---|
| Reproducible images | `docker compose build airflow-init tooling` | PASS — `kest-airflow:3.3.2` and `kest-tooling:0.1.0` built with pinned `pip==26.2.1`; isolated ingestion, dbt and OpenMetadata environments installed |
| Core bootstrap | `make up` | PASS — idempotent Python bootstrap completed and default services became healthy |
| Static/unit | `make lint type test-unit config` and `make dependency-check` | PASS — lint and type checks clean; 6 unit tests passed; Compose and pinned dependency validation passed |
| Core integration | `make test-integration smoke health` | PASS — 11 tests passed, 1 demo-only test deselected; Gold mart row counts `2/2/3`, net revenue `160.00` |
| Failure and RBAC | `make failure-test` | PASS — 9 tests passed, including retry/idempotency, role denial and publish safety cases |
| DE workspace | `make dev-init DEVELOPER=alice` then `make dbt-build DEVELOPER=alice` | PASS — isolated `dev_alice` database provisioned; dbt completed `25/25` |
| DAG definitions | `airflow dags list-import-errors --output=json` | PASS — `[]`; exactly 5 supported DAGs discovered |
| Public Airflow UI | Browser login through `http://127.0.0.1:8080` | PASS — login and DAG pages remain on the host-reachable URL; Celery uses the separate internal execution API URL |
| TaskFlow ingestion | Airflow run `manual__public_url_regression_20261011` of `source_ingestion` | PASS — external-Python ingestion tasks completed with run state `success` after public/internal URL separation |
| Object storage | S3 integration suite against SILO | PASS — credential allow/deny, versioning, put/head/get/list, Parquet round trip, checksum and no-delete policy verified |
| Backup/restore | `make backup` then `make restore-test` | PASS — 38 objects, 21 PostgreSQL rows, raw manifests and all three Gold mart checksums verified in isolated restore targets |
| OpenMetadata UI | HTTP checks for `/` and `/healthcheck` on `127.0.0.1:8585` | PASS — both returned HTTP 200; the web UI is served by the full OpenMetadata server |
| Catalog sync | `make catalog-sync` | PASS — verified 3 marts, 12 dbt tests, glossary terms `NetRevenue` and `ConversionFunnel`, 2 Superset dashboards, 5 Airflow pipelines and table-level lineage |
| Scheduled catalog path | Airflow `metadata_sync` runs `manual__full_catalog_20261011` and `scheduled__2026-10-11T04:00:00+00:00` | PASS — both finished with run state `success` using the isolated OpenMetadata ingestion environment |
| Role documentation | local Markdown link check and captured product UIs | PASS — DE/DA cookbooks resolve all local links and include current Airflow, Superset, OpenMetadata, SILO and ClickHouse screenshots |

Core and catalog services remain running so Airflow, Superset, the SILO console
and OpenMetadata can be inspected. Demo source and recovery-only services were
stopped after their tests. This is a production-disciplined single-host
deployment; it is not HA and it does not provide host-loss disaster recovery.
