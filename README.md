# Kest local environment

Local data-platform environment with three isolated PostgreSQL instances, MinIO,
Lakekeeper, RisingWave, Airflow and the optional CyberMarket workload. The
workload includes reproducible Bronze history, committed raw CDC and a finite
Silver/Gold Iceberg batch.

## Layout

```text
docker-compose.yml              Services, network, volumes and resource limits
.env.example                    Configuration template; credentials live in .env
Makefile                        Start, stop, restart and logs
docker/
  airflow/start.sh              Local UI credentials and standalone startup
  airflow/dags/                 Manually triggered finite batch DAG
  lakekeeper/bootstrap.py      Idempotent empty bucket/warehouse bootstrap
  risingwave/risingwave.toml    Small single-node storage/cache settings
workload/                       CyberMarket generators, ingestion and transforms
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

The five named volumes preserve PostgreSQL, MinIO and RisingWave state. Airflow
metadata lives in `postgres-airflow-data`; UI credentials are restored from `.env`.
RisingWave's `[system]` storage sizes are set when its volume is first initialized.
Container memory limits total 5.75 GiB; actual idle use is lower.

## CyberMarket workload

The optional workload uses a disposable Python tool container so generators and
CDC remain stopped unless explicitly invoked. Its environment is locked in
`requirements.txt`; `make env` creates the same local Python environment for
DuckDB, PyIceberg, Parquet/S3 and PostgreSQL development.

CyberMarket's manual Airflow DAG runs a finite DuckDB batch. Each run writes
immutable versioned Silver/Gold namespaces through Lakekeeper, then atomically
updates `iceberg/_kest_batches/current.json`. Consumers resolve namespaces from
that pointer. RisingWave remains idle until a streaming phase needs it.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the current end-to-end
architecture and business rules, and [workload/README.md](workload/README.md) for
the concise object layout and commands.
