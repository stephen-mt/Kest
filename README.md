# Kest local environment

Local data-platform environment with isolated PostgreSQL instances, MinIO,
Lakekeeper, RisingWave, Airflow, optional NiFi/Trino services and the optional CyberMarket workload. The
workload includes reproducible Bronze history, committed raw CDC and a finite
Silver/Gold Iceberg batch.

## Layout

```text
docker-compose.yml              Services, network, volumes and resource limits
.env.example                    Configuration template; credentials live in .env
Makefile                        Start, stop, restart and logs
docker/
  airflow/start.sh              Local UI credentials and standalone startup
  airflow/dags/                 TaskFlow DAGs
  data-jobs/Dockerfile          Shared locked Python job runtime
  lakekeeper/bootstrap.py      Idempotent empty bucket/warehouse bootstrap
  nifi/
    bootstrap.py, check.py     Small flow lifecycle entry points
    kest_nifi/                 API client, flow model and NiFi resource manager
  trino/etc/                   Iceberg/PostgreSQL catalogs and bootstrap SQL
  risingwave/risingwave.toml    Small single-node storage/cache settings
workload/                       CyberMarket generators, ingestion and transforms
scenarios/delivery_ops/         Isolated logistics source, batch and streaming jobs
```

## Start

Requires Docker with Compose v2, Make and Python 3. Run commands from the repo root.

On a fresh checkout, copy `.env.example` to `.env` and replace the placeholders.
Keep database credentials URL-safe: letters, digits and underscores. Generate
`AIRFLOW_FERNET_KEY` with:

```sh
python3 -c 'import base64, secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())'
chmod 600 .env
```

An existing local `.env` can be reused. It is excluded from Git.

```sh
make start
```

This waits for PostgreSQL and MinIO, runs Lakekeeper's metadata migration in a
temporary container, starts all seven services, then ensures one empty bucket
and warehouse exist. Airflow initializes its own metadata. Repeated starts
preserve the existing warehouse. Use `make start` for the first launch so the
migration and bootstrap steps run in the required order.

## Access

| Service | Local endpoint | Credentials in `.env` |
| --- | --- | --- |
| Airflow | http://127.0.0.1:8080 | `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD` |
| Lakekeeper UI | http://127.0.0.1:8181/ui/ | Local authentication disabled |
| MinIO API | http://127.0.0.1:9000 | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` |
| MinIO Console | http://127.0.0.1:9001 | Same MinIO credentials |
| RisingWave SQL | `127.0.0.1:4566` | User `root`, database `dev`, no password |
| NiFi (optional) | https://127.0.0.1:8090/nifi/ | `NIFI_USERNAME`, `NIFI_PASSWORD` |
| Trino (optional) | http://127.0.0.1:8081 | Local authentication disabled |

PostgreSQL ports stay inside `kest-net`. Lakekeeper connects to MinIO through
`http://minio:9000/` on that network. Airflow uses `LocalExecutor`; RisingWave uses
local SQLite metadata and filesystem storage in `risingwave-data`.

## Operate

```sh
make stop       # Stop containers, retain state
make restart    # Stop and start with metadata/bootstrap checks
make logs       # Follow service logs
docker compose ps
```

Start the optional ingestion UI and shared SQL engine with `make platform-up`.
After reviewing the CDC table allowlist, `make platform-cdc-up` also starts
Debezium. Use `make platform-down` to stop all three optional services.

Named volumes preserve PostgreSQL, MinIO, RisingWave, NiFi and Debezium state.
Airflow metadata lives in `postgres-airflow-data`; UI credentials are restored
from `.env`. RisingWave's `[system]` storage sizes are set when its volume is first
initialized. Core container memory limits total 5.75 GiB; optional profiles add
their own limits.

## CyberMarket workload

The optional workload uses a disposable Python tool container so generators and
CDC remain stopped unless explicitly invoked. Its environment is locked in
`requirements.txt`; `make env` creates the same local Python environment for
DuckDB, PyIceberg, Parquet/S3 and PostgreSQL development.

CyberMarket's manual Airflow DAG runs a finite DuckDB batch. Each run writes
immutable versioned Silver/Gold namespaces through Lakekeeper, then atomically
updates `iceberg/_kest_batches/current.json`. Consumers resolve namespaces from
that pointer. RisingWave remains idle until a streaming phase needs it.

See [docs/CYBERMARKET_END_TO_END.md](docs/CYBERMARKET_END_TO_END.md) for the current end-to-end
architecture and business rules, and [workload/README.md](workload/README.md) for
the concise object layout and commands. See
[docs/NIFI_CDC_AND_TRINO_SQL.md](docs/NIFI_CDC_AND_TRINO_SQL.md) for source onboarding through
NiFi/Debezium and shared SQL through Trino.

## DeliveryOps end-to-end scenario

DeliveryOps is a separate logistics source used to exercise PostgreSQL CDC,
NiFi raw landing, RisingWave materialized views, Airflow TaskFlow jobs, DuckDB,
Iceberg, Lakekeeper and Trino. It uses bucket `mini-cybet-delivery` and warehouse
`delivery-ops`; its commands do not reset the CyberMarket workload.

See [docs/DELIVERY_OPS_END_TO_END.md](docs/DELIVERY_OPS_END_TO_END.md) for the source
schema, 24-task DAG, 35 Iceberg tables, business rules, runtime commands and the
governance integration path.

The documentation map is in [docs/README.md](docs/README.md). The production gap
and target planes are described in
[docs/DEV_TO_PRODUCTION_GUIDE.md](docs/DEV_TO_PRODUCTION_GUIDE.md).
