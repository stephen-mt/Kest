# Daily product workflow

## Data engineer

A DE normally uses Git, dbt and the Airflow/OpenMetadata UIs. They do not edit
Compose, create production users, copy Parquet files or write Gold tables.

### Change a model

1. Create the private workspace once with `make dev-init DEVELOPER=alice`.
2. Edit SQL and YAML under `transformations/models`.
3. Run `make dbt-build DEVELOPER=alice`. Models are built in `dev_alice`; the
   generated `de_alice` user is denied production writes by ClickHouse.
4. Open a pull request. CI runs lint, contracts, dbt tests, DAG parsing and the
   core integration path.
5. After review, deploy the same commit to staging. A production promotion runs
   dbt against `gold_candidate`; only `publish_service` can atomically exchange
   a tested candidate with `gold`.

Alice and Bob can change the same model concurrently because their databases
are `dev_alice` and `dev_bob`. Neither environment is a schema prefix selected
by convention: grants enforce the boundary.

### Add a REST source

```bash
make source-new SOURCE=crm BASE_URL=https://crm.example/api \
  ENDPOINT=/customers PRIMARY_KEY=customer_id
```

This creates and validates one source YAML. The common dlt runner, immutable
S3 landing, manifest/checkpoint handling and ClickHouse loader need no DAG edit.
New data first lands in the generic append-only raw table; the DE adds a typed
staging model and its contract before exposing it to analysts. A source with
unusual auth or pagination needs a reviewed adapter, not a second ingestion
framework.

### Operate a failed run

Use Airflow at `http://127.0.0.1:8080` to inspect the failed task and retry it.
Tasks pass only identifiers through XCom; payloads stay in object storage. For
a historical correction, trigger `controlled_backfill` with source, dates,
reason and `dry_run=true` first. Replay is safe at the batch boundary.

Use OpenMetadata at `http://127.0.0.1:8585` to check ownership, tests, lineage
and dashboard impact before changing a shared mart.

## Data analyst

A DA starts with `make da`, then works in Superset at
`http://127.0.0.1:8088`:

1. Find the relevant Gold mart in OpenMetadata, including grain, owner,
   freshness and upstream lineage.
2. Explore the corresponding Superset dataset or open a maintained dashboard.
3. Build a chart from canonical columns already calculated by dbt. Do not
   redefine net revenue or funnel semantics in dashboard SQL.
4. If a metric is missing, request a mart/model change from its owner; once the
   promoted model is published, all dashboards see the same definition.

The Superset database identity has `SELECT` on `gold` only. A DA can explore
and save BI content but cannot insert, alter or drop warehouse tables.

## Platform operator

The operator owns credentials, availability and promotion—not business SQL.

- `make up`, `make health`, `make down`: lifecycle and deep health.
- `make catalog` / `make catalog-down`: optional catalog UI and sync schedule.
- `make backup`, copy the bundle off-host, `make restore-test`: recovery.
- `make dependency-check`: verify locked Python and Compose dependencies.
- Review capacity, Airflow failures, PostgreSQL/ClickHouse logs and backup age.

OpenMetadata's bundled `admin@open-metadata.org / admin` login is acceptable
only for local evaluation. A real production deployment must replace it with
organizational SSO (or at minimum remove/rotate the default admin) before users
are admitted.
