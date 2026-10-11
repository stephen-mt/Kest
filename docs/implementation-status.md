# Implementation status

Verified at `2026-10-11T02:26:23Z` against Git commit `16919ba` on
`codex/production-oss-platform`.

| Gate | Command | Result |
|---|---|---|
| Core bootstrap | `make up` | PASS — all default services healthy |
| Static and core tests | `make lint type test dependency-check` | PASS — 4 unit, 11 core integration/security, 1 demo test deselected |
| Data path | `make pipeline smoke health` | PASS — dbt 25/25; Gold rows 2/2/3; net revenue `160.00`; PostgreSQL, ClickHouse and S3 healthy |
| Failure/RBAC suite | `make failure-test` | PASS — 9 tests |
| SILO policy | object-store integration test | PASS — versioning enabled; reader cannot write; raw writer cannot delete |
| Recovery | `make backup restore-test` | PASS — 38 raw objects, 21 PostgreSQL source rows and all three Gold mart checksums verified in isolated targets |
| Airflow execution | manual `source_ingestion` trigger | PASS — run `manual__2026-10-11T02:23:28.324949+00:00` finished `success` |
| DAG imports | `airflow dags list-import-errors --output=json` | PASS — `[]`; six DAGs discovered |
| BI security | Superset `/health` headers | PASS — CSP, `X-Frame-Options` and `X-Content-Type-Options` present |
| Optional catalog | `make catalog-up` then `ops/metadata_sync.sh` | PASS — ClickHouse/dbt/Airflow/Superset ingestion 100%; 3 marts, 12 tests, 2 terms, 2 dashboards, 6 pipelines and table lineage verified |

No gate is recorded as PASS solely from a container health status. Catalog and
demo/recovery profiles were stopped after verification; the core remains
running. The deployment is single-host and therefore is not HA or host-loss DR.
