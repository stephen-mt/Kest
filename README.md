# Kest

Kest is a local data platform for exercising source ingestion, raw landing,
batch and streaming transforms, Apache Iceberg publication, orchestration, and
SQL access on one Docker host. It includes two isolated example data products:
CyberMarket and DeliveryOps.

The default stack runs PostgreSQL, MinIO, Lakekeeper, RisingWave, and Airflow.
NiFi, Debezium, and Trino are optional profiles. All persistent state lives in
named Docker volumes; job containers are disposable.

## Quick start

Requirements: Docker with Compose v2, GNU Make, Python 3, at least 8 GiB of free
disk, and enough memory for the selected services.

```sh
cp .env.example .env
# Replace every change_me/replace_with value in .env.
python3 -c 'import base64, secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())'
make doctor
make demo
```

`make demo` starts the core platform, initializes the catalog and bucket, creates
the CyberMarket source, generates a 128 MiB Bronze fixture, publishes Silver and
Gold Iceberg tables, and validates the result. It is safe to rerun. Use
`make demo-full` only when you want the multi-gigabyte fixture.

Stop the platform without deleting data:

```sh
make down
```

## Repository map

```text
.
├── src/kest/                     Shared storage and Iceberg primitives
├── workloads/
│   ├── cybermarket/              Marketplace source, CDC, batch, validation
│   └── delivery_ops/             Logistics source and governed data product
├── orchestration/airflow/dags/   TaskFlow DAG definitions
├── deploy/local/
│   ├── compose.yaml              Core local platform
│   ├── compose.platform.yaml     NiFi, Debezium, and Trino
│   ├── compose.cybermarket.yaml  CyberMarket job runtime
│   ├── compose.delivery-ops.yaml DeliveryOps services and job runtime
│   └── services/                 Dockerfiles and service configuration
├── tests/unit/                   Fast deterministic tests
├── scripts/                      Checkout diagnostics and command wrapper
├── Makefile                      Stable operator entry points
└── .env.example                  Complete local configuration template
```

Shared code under `src/kest` has no dependency on a workload. Each workload owns
its source model, transforms, contracts, and lifecycle commands. Airflow imports
workload entry points and contains no transform SQL or business rules. Compose
and service-specific files stay under `deploy/local`.

See [ARCHITECTURE.md](ARCHITECTURE.md) for component boundaries and data flow,
[CONTRIBUTING.md](CONTRIBUTING.md) for repository conventions,
[CyberMarket](workloads/cybermarket/README.md), and
[DeliveryOps](workloads/delivery_ops/README.md) for workload-specific operation.

## Common commands

| Command | Result |
| --- | --- |
| `make doctor` | Check `.env`, Docker, Compose, daemon access, and disk space |
| `make demo` | Run the small CyberMarket path from source through Gold |
| `make demo-full` | Generate the full Bronze fixture and publish it |
| `make up` / `make down` | Start or stop the core platform while retaining state |
| `make status` | Show containers from every local profile |
| `make platform-up` | Start NiFi and Trino |
| `make platform-cdc-up` | Start NiFi, Trino, and the allowlisted Debezium source |
| `make platform-down` | Stop the optional shared services |
| `make check` | Validate Compose, formatting, lint, and unit tests |

The wrapper `./scripts/kest` exposes the first-run commands for people who do
not need the complete Make target list.

## Local endpoints

| Service | Endpoint | Authentication |
| --- | --- | --- |
| Airflow | <http://127.0.0.1:8080> | `.env` Airflow credentials |
| Lakekeeper | <http://127.0.0.1:8181/ui/> | Disabled for local development |
| MinIO API | <http://127.0.0.1:9000> | `.env` MinIO credentials |
| MinIO console | <http://127.0.0.1:9001> | `.env` MinIO credentials |
| RisingWave | `127.0.0.1:4566` | `root`, database `dev`, no password |
| NiFi | <https://127.0.0.1:8090/nifi/> | `.env` NiFi credentials |
| Trino | <http://127.0.0.1:8081> | Disabled for local development |

PostgreSQL is internal to the `kest-net` Docker network. The local environment
uses loopback endpoints, simple authentication, and Lakekeeper `allowall`; it is
a development system rather than a production deployment template.

## State and cleanup

`make down` stops containers and preserves PostgreSQL, MinIO, RisingWave, NiFi,
and Debezium volumes. Regular development commands do not delete those volumes.
Inspect state with `make status` and Docker's volume commands before any manual
cleanup. CyberMarket uses bucket `mini-cybet`; DeliveryOps uses the separate
`mini-cybet-delivery` bucket and never resets CyberMarket data.
