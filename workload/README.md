# CyberMarket workload

The workload is opt-in and runs in disposable tool containers. The seven local
infrastructure services remain the only long-lived default services.

## Source layout

```text
workload/
  core/           # environment settings and S3 client
  cybermarket/    # schema bootstrap, SQL and transactional event writes
  landing/        # upload-before-ack raw CDC writer
  lakehouse/      # Lakekeeper catalog and Iceberg helpers
  pipelines/      # CDC, live generator, bronze history and finite batch jobs
  validation/     # smoke tests plus lifecycle and batch verification
```

Long-running entry points live in `pipelines/`; domain code does not own process
signals. Configuration is read explicitly at startup, so importing a module
never requires environment secrets.

## Data model

PostgreSQL is the current operational state. Parquet is analytical history from
the same CyberMarket universe, and raw CDC contains the new PostgreSQL mutations
that continue it. The shared entity domains are 10 `PLAT-NNN` markets, 1,000
`SELLER-NNNNNN` vendors, 10,000 `BUYER-NNNNNNN` buyers and 4,000 composite
product keys. Historical fact IDs use `HIST`; new operational facts use `LIVE`,
which prevents key collisions while preserving one identifier convention.

The 5 GiB target is concentrated in event tables. Fact cardinalities follow the
live workload: two items, one payment and one risk row per purchase; four
sessions per three purchases; one prediction per three purchases. References
span the full transaction domain and product sales rotate across all four
products per vendor. Mixed casing, quoted identifiers, composite keys, JSONB and
numeric-looking text remain intentional parts of the raw schema.

## Object layout

```text
s3://mini-cybet/
  landing/postgres-source/data/<table>/batch-<sha256>.jsonl.gz
  landing/postgres-source/commits/<sha256>.json
  bronze/history/<table>/*.parquet
  bronze/history/_manifest.json
  iceberg/_kest_batches/<batch-id>.json
  iceberg/_kest_batches/current.json
  iceberg/                         # Lakekeeper-managed versioned table data
```

Raw landing records are immutable gzip-compressed JSON Lines. Object keys and
payloads are deterministic, writes use `If-None-Match`, and MinIO bucket
versioning is enabled. The CDC writer uploads every table object, writes one
completion manifest, then advances its PostgreSQL slot. A partial upload has no
commit and is ignored. Retries verify byte-identical content instead of
overwriting it.

## Batch lakehouse

The finite batch captures every PostgreSQL table in one `REPEATABLE READ, READ
ONLY` transaction and records its WAL LSN, timestamp and row counts. Silver
keeps the ten source-shaped tables and adds `cdc_events`, a deduplicated Iceberg
audit table built only from committed raw batches with a through-LSN checkpoint.
Fact tables combine Bronze history with the consistent source snapshot.

Every run writes new immutable `silver_<batch-id>` and `gold_<batch-id>`
namespaces. After all checks and the immutable batch manifest succeed, one
conditional PUT changes `current.json`. Concurrent publishers cannot overwrite
one another, and consumers never observe a partly replaced layer. Old versions
remain available for rollback and may be removed later under a retention policy.
JSONB values are stored as JSON strings because Iceberg has no native JSON type.

Gold contains four small query models:

| Table | Grain and purpose |
| --- | --- |
| `daily_market_metrics` | Date and platform; volume, GMV, cross-border, payment and risk metrics |
| `vendor_risk_summary` | Vendor; transaction, buyer, GMV and fraud-risk metrics |
| `buyer_360` | Buyer; purchase, session, checkout and lifetime-value metrics |
| `product_performance` | Composite product key; transactions, units and revenue |

Gold casts monetary aggregates to decimal and timestamps to UTC-aware values.
The raw PostgreSQL/Silver schema remains compatible with the source contract.

## Commands

```sh
make workload-setup   # exact schema plus causal idempotent dimension seed
make history          # resumable generation of approximately 5 GiB Parquet
make history-reset    # replace only the reproducible Bronze fixture
make workload-check-history-deep # scan all facts for semantic contradictions
make cdc-test         # one second: 20 events, land WAL, validate, then exit
make cdc              # foreground CDC; start this before live generation
make generate         # foreground fixed-rate 20 events/sec generator
make cdc-drain        # land pending changes and exit
make batch            # build and publish Silver/Gold Iceberg tables directly
make batch-check      # validate Iceberg schemas, counts and Gold rollups
make batch-airflow    # trigger the same two-step batch through Airflow
make airflow-dag-check # list parsed DAGs and import errors
make workload-check           # State A: schema and canonical operational seed
make workload-check-history   # State B: State A plus Parquet and manifest
make workload-check-cdc       # State C: State A plus slot and raw landing
make test              # deterministic rule and history-resume unit tests
```

The checks are lifecycle-aware: setup does not require history or CDC, history
does not require a replication slot, CDC does not require Parquet, and
`batch-check` requires the complete published lakehouse.

Run `make cdc` and `make generate` in separate terminals for a live demo. Stop
either with Ctrl-C. The CDC replication slot persists while the process is off,
but PostgreSQL caps retained slot WAL at 1 GiB. Drain promptly after generating
events; an overrun invalidates the slot and requires deliberate recreation.

The one-second event frame always contains 8 buyer sessions, 6 purchases,
3 payment updates, 2 risk predictions and 1 transaction status update. Each
purchase is one database transaction with exactly the six operations specified
in the workload contract, including two product lines.

The `cybermarket_batch` DAG has no schedule and permits one active run. Airflow
therefore orchestrates batch work only when explicitly triggered; tasks have
bounded retries and execution timeouts. Lakekeeper is the Iceberg REST catalog
and MinIO owns the table data. RisingWave remains unused in this batch.

This remains a local development environment. It uses local credentials,
Lakekeeper `allowall`, Airflow simple auth and loopback HTTP endpoints. Production
deployment requires separate service identities, TLS/OIDC, backups and external
monitoring; the repository does not claim those guarantees.
