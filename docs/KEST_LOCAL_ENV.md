# Kest Local Environment

> Phase contract: this document specifies the infrastructure baseline only.
> Workload components built on top of that baseline are described in
> `workload/README.md`; the prohibitions below apply to infrastructure bootstrap,
> not to the current repository as a whole.

## Scope

This file defines **infrastructure only** for the local Kest environment.

Do not create or modify:
- workload data;
- database tables;
- sample data;
- Airflow DAGs;
- CDC sources/sinks;
- RisingWave tables or materialized views;
- Iceberg tables;
- ingestion/transform jobs;
- semantic/quality logic;
- Kur integration.

Goal: start the minimum set of local services with persistent state and low resource usage.

---

## Required Services

Run exactly these long-lived services:

```text
postgres-source
postgres-airflow
postgres-catalog
minio
lakekeeper
risingwave
airflow
```

Architecture:

```text
postgres-source        risingwave
     future CDC        standalone


postgres-airflow  <──  airflow


postgres-catalog  <──  lakekeeper  ──>  minio
```

DuckDB and Kest code are not daemons and are not part of this environment stack.

---

## postgres-source

Dedicated PostgreSQL instance for the live source.

Keep it isolated from Airflow and Lakekeeper metadata.

Prepare PostgreSQL for future logical-replication CDC:

```text
wal_level=logical
max_wal_senders=4
max_replication_slots=4
```

Persistent volume:

```text
postgres-source-data
```

Target memory:

```text
256-384 MiB
```

Do not create workload tables or seed data.

---

## postgres-airflow

Dedicated PostgreSQL metadata backend for Airflow.

Persistent volume:

```text
postgres-airflow-data
```

Target memory:

```text
~256 MiB
```

Do not share this instance with other services.

---

## postgres-catalog

Dedicated PostgreSQL metadata backend for Lakekeeper.

Persistent volume:

```text
postgres-catalog-data
```

Target memory:

```text
~256 MiB
```

Do not share this instance with other services.

---

## MinIO

Single-node local S3-compatible object storage.

Expose only on localhost:

```text
127.0.0.1:9000   S3 API
127.0.0.1:9001   Console
```

Persistent volume:

```text
minio-data
```

Target memory:

```text
256-384 MiB
```

Do not upload data or create sample objects.

Only create storage/bootstrap objects that are strictly required for Lakekeeper to start.

---

## Lakekeeper

Single Lakekeeper instance providing an Iceberg REST catalog.

Dependencies:

```text
postgres-catalog
minio
```

Expose:

```text
127.0.0.1:8181
```

Target memory:

```text
256-512 MiB
```

Local-only configuration:

```text
no HA
no Keycloak
no external OIDC
no reverse proxy
no TLS termination service
```

Configure only enough metadata/database/object-storage settings for Lakekeeper to become healthy.

Do not create application namespaces or Iceberg tables.

---

## RisingWave

Run RisingWave in the smallest supported standalone/single-node mode.

Expose SQL endpoint:

```text
127.0.0.1:4566
```

Persistent local state:

```text
risingwave-data
```

Target memory:

```text
1-1.5 GiB
```

Do not add:

```text
Kafka
Redpanda
Debezium
etcd
separate RisingWave nodes
external RisingWave metadata services
```

Do not define CDC sources, sinks, tables, materialized views, or streaming queries.

---

## Airflow

Run the smallest local single-machine Airflow configuration.

Metadata backend:

```text
postgres-airflow
```

Expose:

```text
127.0.0.1:8080
```

Target memory:

```text
1-1.5 GiB
```

Use local execution only.

Disable:

```text
example DAGs
Celery
Redis
Flower
remote workers
Kubernetes executor
```

Do not create any DAGs.

---

## Docker Network

Use one private Compose network:

```text
kest-net
```

Services communicate using Docker Compose service names.

PostgreSQL ports should remain internal by default.

Only expose these developer endpoints:

```text
Airflow        127.0.0.1:8080
Lakekeeper     127.0.0.1:8181
MinIO API      127.0.0.1:9000
MinIO Console  127.0.0.1:9001
RisingWave     127.0.0.1:4566
```

---

## Persistent Volumes

Keep separate volumes:

```text
postgres-source-data
postgres-airflow-data
postgres-catalog-data
minio-data
risingwave-data
```

Never share PostgreSQL data directories or service state volumes.

---

## Health and Startup

Add lightweight Docker health checks.

Required readiness dependencies:

```text
postgres-airflow  -> airflow
postgres-catalog  -> lakekeeper
minio             -> lakekeeper
```

`postgres-source` and `risingwave` may start independently.

Use Compose health conditions where practical.

Do not add a separate service-discovery or monitoring stack.

---

## Resource Policy

This stack is for a laptop, not production.

Approximate targets:

| Service | Memory |
|---|---:|
| postgres-source | 256-384 MiB |
| postgres-airflow | ~256 MiB |
| postgres-catalog | ~256 MiB |
| minio | 256-384 MiB |
| lakekeeper | 256-512 MiB |
| risingwave | 1-1.5 GiB |
| airflow | 1-1.5 GiB |

Target total steady-state memory:

```text
~3-5 GiB
```

Prefer low concurrency and small worker/thread counts.

Do not reserve large heaps or CPU pools.

---

## Environment Variables

Put configuration and local credentials in:

```text
.env
```

Provide safe placeholders in:

```text
.env.example
```

Keep variables grouped by service:

```text
POSTGRES_SOURCE_*
POSTGRES_AIRFLOW_*
POSTGRES_CATALOG_*
MINIO_*
LAKEKEEPER_*
RISINGWAVE_*
AIRFLOW_*
```

Use simple local-development credentials.

Do not introduce Vault or another secrets service.

---

## Expected Files

Infrastructure implementation should stay small:

```text
docker-compose.yml
.env.example
docker/
  <only minimal service-specific config when required>
```

A tiny `Makefile` or shell script is acceptable only for:

```text
start
stop
restart
logs
```

Do not create workload/application scaffolding.

---

## Explicitly Out of Scope

Do not add:

```text
Spark
Trino
Flink
Kafka
Redpanda
Debezium
DataHub
OpenMetadata
Nessie
Redis
Celery
Flower
Keycloak
Prometheus
Grafana
Loki
Jaeger
OpenTelemetry Collector
nginx
Traefik
PgBouncer
Schema Registry
```

Do not add a service just because it would normally appear in a production deployment.

---

## Completion Criteria

Environment setup is complete when:

1. all seven required services start;
2. all required health checks pass;
3. Airflow uses `postgres-airflow`;
4. Lakekeeper uses `postgres-catalog` and MinIO;
5. RisingWave runs in standalone/single-node mode;
6. `postgres-source` is prepared for future logical replication;
7. host-facing ports bind to `127.0.0.1`;
8. persistent service state survives restart;
9. no workload, sample, DAG, CDC pipeline, Iceberg table, or demo data is created.

**Keep it infrastructure-only and minimal.**
