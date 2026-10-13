# Kest

Kest is a compact OSS data platform for a small data team. Data engineers get
one governed path from source to Gold, data analysts get searchable datasets
and a BI workspace, and platform engineers get one identity, release and
observability plane.

It is designed for a dedicated Docker Compose host. It is not a claim of
multi-host HA or unlimited scale.

![Kest platform architecture](assets/platform-architecture.png)

## What users actually do

| Role | Primary surfaces | Normal workflow |
| --- | --- | --- |
| Data engineer | Airflow, dbt, OpenMetadata | Add a source, develop in an isolated `dev_<name>` database, open a PR, then inspect scheduled runs and lineage |
| Data analyst | OpenMetadata, Superset | Find the owner and definition of a Gold dataset, explore canonical columns, then build or consume a dashboard |
| Platform engineer | Keycloak, Grafana, Airflow | Manage roles, watch freshness and release SLOs, investigate failed runs, migrate, back up or roll back |

DEs and DAs do not assemble connectors or copy credentials between tools. One
Keycloak identity maps platform roles into Airflow, Superset, OpenMetadata and
Grafana. Database permissions remain the enforcement boundary underneath the
UIs.

## Platform model

### Data plane

1. dlt extracts PostgreSQL, REST and file sources into immutable, versioned
   Parquet batches in SILO.
2. PostgreSQL Control records batch objects, checksums, watermarks and source
   checkpoints. A checkpoint advances only after its batch is durable.
3. The loader verifies the manifest and loads ClickHouse idempotently using the
   batch ID as the deduplication token.
4. dbt builds staging, core and mart models. Developer builds are isolated in
   `dev_<developer>`; production builds target Gold candidates.
5. Publishing validates a candidate and atomically exchanges one mart at a
   time into Gold. Its prepared checksum is immutable: after a worker crash,
   retry reconciles the recorded checksum against Gold before deciding whether
   an exchange is still required. Publication state, release identity and
   rollback history are durable control-plane records.

The three empty Gold serving contracts are created during migration, so
Superset can bootstrap datasets and dashboards before the first publication.
They contain no demo rows; the first successful publish atomically replaces
each contract table with its validated candidate.

### Control plane

- Apache Airflow runs ingestion, transformation, publishing, bounded backfills
  and metadata synchronization.
- PostgreSQL Control owns versioned migrations, manifests, checkpoints,
  advisory locks, pipeline/run correlation, publication state and release
  audit.
- GitHub CI is the delivery gate. Immutable images are built only after the CI
  workflow for the exact `main` commit succeeds; the workflow checks out and
  labels that commit SHA rather than whichever revision happens to be current.
- Keycloak supplies SSO and the `platform-admin`, `data-engineer` and
  `data-analyst` roles.
- Prometheus collects service and platform-state metrics. Alertmanager groups
  actionable alerts and optionally relays them to
  `PLATFORM_ALERT_WEBHOOK_URL`. Grafana is the operator console.
- OpenMetadata ingests ClickHouse, dbt, Airflow and Superset metadata so owners,
  glossary terms, tests, dashboards and supported lineage are searchable.

### Experience layer

- Airflow is the DE execution surface; source and publish mechanics remain in
  reusable platform code.
- OpenMetadata is the common discovery surface for DE and DA.
- Superset is the DA workspace and reads Gold with a read-only ClickHouse
  identity. Business definitions stay in dbt rather than being reimplemented
  in individual charts.
- Grafana shows service health, stuck batches and runs, source and dataset
  freshness, release state, backup verification age, Gold row counts and host
  capacity.

## Start locally

Requirements: Linux, Docker Engine with Compose v2, GNU Make and Python 3.12.
Allow roughly 12 CPU, 16 GiB RAM and 60 GiB free disk when every optional
profile is enabled.

```bash
make up                    # core data platform and SSO
make credentials           # local URLs and generated local-only logins
make catalog               # OpenMetadata UI plus metadata synchronization
make observability-up      # Prometheus, Alertmanager and Grafana
```

`make up` generates `.env` and runtime policy files with restrictive local
permissions, applies checksum-protected migrations, reconciles Keycloak
clients, and performs idempotent service bootstrap. It does not seed demo data
or run a business pipeline.

The ecommerce workload is only an acceptance fixture. Run `make demo` when you
explicitly want it; normal platform startup remains empty.

## Product surfaces

All development ports bind to `127.0.0.1`.

| Product | Local URL | Primary audience | Purpose |
| --- | --- | --- | --- |
| Apache Airflow | [http://127.0.0.1:8080](http://127.0.0.1:8080) | DE / platform | Schedule and inspect pipelines, backfills and metadata sync |
| Apache Superset | [http://127.0.0.1:8088](http://127.0.0.1:8088) | DA | Explore governed Gold data and publish dashboards |
| OpenMetadata | [http://127.0.0.1:8585](http://127.0.0.1:8585) | DE / DA | Search datasets, owners, tests, glossary and lineage |
| Grafana | [http://127.0.0.1:3000](http://127.0.0.1:3000) | Platform / DE | Operate SLOs, releases, freshness and capacity |
| Kest SSO | [http://127.0.0.1:8180](http://127.0.0.1:8180) | Platform | Manage local identities, sessions and platform roles |
| SILO Console | [http://127.0.0.1:9001](http://127.0.0.1:9001) | DE / platform | Inspect raw buckets and object versions |
| ClickHouse Diagnostics | [http://127.0.0.1:8123/dashboard](http://127.0.0.1:8123/dashboard) | DE / platform | Inspect database health and query activity |
| Prometheus | [http://127.0.0.1:9090](http://127.0.0.1:9090) | Platform | Query metrics and inspect alert rules |
| Alertmanager | [http://127.0.0.1:9093](http://127.0.0.1:9093) | Platform | Group, route and silence alerts |

Run `make credentials` for the local usernames and passwords. Do not expose
these loopback services directly to a public network.

## Data engineer workflow

Create an isolated ClickHouse development database and build the dbt project:

```bash
make de DEVELOPER=alice
```

Scaffold a REST source without inventing a new configuration language:

```bash
make source-new \
  SOURCE=crm \
  BASE_URL=https://crm.internal/api \
  ENDPOINT=customers \
  TABLE=customers \
  PRIMARY_KEY=id
```

Then:

1. Configure the generated YAML with the native dlt REST options and store
   credentials outside Git.
2. Add staging/core/mart SQL and contracts under `transformations/models/`.
3. Iterate with `make dbt-build DEVELOPER=alice`.
4. Open a PR. CI validates Compose, lint/type checks, starts the core platform,
   runs the acceptance path and checks Airflow DAG imports.
5. After promotion, use Airflow for normal runs. Use the
   `controlled_backfill` DAG with `source`, `start`, `end`, `reason` and
   `dry_run` for bounded recovery work.

Production writes are deliberately split: the pipeline identity may build raw,
staging and candidates, while the publish identity alone can exchange Gold.

## Data analyst workflow

```bash
make da
```

1. Sign in through Kest SSO.
2. Start in OpenMetadata to find the dataset description, owner, grain,
   freshness expectation, tests and lineage.
3. Open the governed dataset in Superset and use its canonical dimensions and
   measures.
4. Report definition or freshness issues to the listed owner; do not repair
   production data with ad-hoc writes. The analyst database role can `SELECT`
   Gold but cannot `INSERT`, `ALTER` or `DROP` it.

## Environments and delivery

| Environment | Data/build target | Delivery model | Intended use |
| --- | --- | --- | --- |
| Local DE | `dev_<developer>` | Compose build plus read-only source checkout mounts | Fast isolated development |
| Staging | Gold candidate and staging Gold | Images tagged with the exact Git SHA | Migration, integration and release verification |
| Production | Gold | Promote the same immutable SHA-tagged images | Scheduled ingestion, governed publish and BI |

A successful `Kest CI` run on `main` builds Airflow, tooling and Superset images
for that exact commit, with an OCI revision label, SBOM and provenance. A failed
or cancelled CI run cannot trigger image publication. The protected deployment
workflow materializes a `0600` environment file on a self-hosted runner and
promotes a selected full Git SHA. Deployment rejects mutable image tags,
mismatched environment files, missing secrets and executable checkout mounts,
then applies migrations before starting services. OpenMetadata and
observability are enabled in staging and production deployments.

The expected CI/CD controls outside this repository are:

- protected `main` with required CI and CODEOWNERS review;
- protected `staging` and `production` GitHub Environments;
- `KEST_ENV_FILE` supplied by the environment secret store;
- an explicit approval before the production deployment job;
- off-host retention for backups and registry artifacts.

## Operations

| Command | Effect |
| --- | --- |
| `make health` | Check PostgreSQL, ClickHouse, S3-compatible storage and platform invariants |
| `make migrate` | Apply versioned, checksum-protected PostgreSQL and ClickHouse migrations |
| `make catalog-sync` | Refresh OpenMetadata entities, ownership, tests, dashboards and lineage |
| `make backup` | Back up control metadata, ClickHouse data and raw objects |
| `make restore-test` | Restore into isolated targets and verify counts/checksums |
| `make rollback DATASET=... PUBLICATION_ID=... REASON=...` | Produce a no-change rollback plan |
| `make rollback DATASET=... PUBLICATION_ID=... REASON=... EXECUTE=1` | Execute and audit the selected mart rollback |
| `make observability-down` | Stop only the optional monitoring profile |
| `make catalog-down` | Stop only the optional catalog profile |
| `make down` | Stop all profiles while retaining persistent volumes |

Run `make help` for the complete command list. Production promotion uses
`make deploy ENV=staging` or `make deploy ENV=production` with the matching
protected environment file.

## Repository map

```text
ingestion/         dlt sources, raw manifests, loader and workflow code
orchestration/     Airflow DAG definitions and shared task policy
transformations/   dbt staging, core, marts, contracts and tests
governance/        OpenMetadata ingestion and ownership configuration
serving/           idempotent Superset datasets and dashboard bootstrap
infra/             service config, images, migrations, identity and monitoring
ops/               Python bootstrap, deployment, backup, recovery and sync tools
assets/            README architecture artwork
```

## Production boundaries

Kest provides least-privilege service identities, SSO, immutable release
promotion, idempotent ingestion, audited per-mart publishing, rollback records,
monitoring and executable backup/restore workflows. Its default topology is
still one host, one ClickHouse node, one PostgreSQL server and one SILO node.

Therefore:

- there is no host-loss HA or automatic cross-region disaster recovery;
- the default backup location must be copied off-host for real recovery;
- raw immutability is an application and access-control invariant, not
  regulatory WORM storage;
- atomicity is per mart, not across every Gold mart in one global transaction;
- local loopback bindings are a development boundary, not a substitute for a
  production reverse proxy, TLS and network policy;
- scaling beyond one large ClickHouse node requires a replicated/distributed
  ClickHouse design and a separately tested migration plan.
