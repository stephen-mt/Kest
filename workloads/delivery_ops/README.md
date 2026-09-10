# DeliveryOps workload

DeliveryOps is an isolated logistics data product that exercises a dirty source,
CDC landing, streaming views, staged batch publication, data-quality quarantine,
governance contracts, and business marts. It owns a PostgreSQL source, the
`mini-cybet-delivery` bucket, and the `delivery-ops` Lakekeeper warehouse.

## Lifecycle

```sh
make up                    # core platform and shared catalog
make delivery-up           # DeliveryOps source plus NiFi and Trino
make delivery-setup        # apply source schema
make delivery-seed         # deterministic dirty operational data
make delivery-source-check # validate expected source defects and contract
make delivery-batch        # stage Bronze/Silver/Gold, quality gate, publish
make delivery-check        # validate the current published run
```

The Airflow DAG exposes the same batch as separate source-contract, Bronze,
Silver, Gold, quality, and atomic-publish tasks:

```sh
make delivery-airflow-up
make delivery-airflow
```

For the live path, start the allowlisted Debezium-to-NiFi CDC connector, emit a
finite event set, and create RisingWave objects:

```sh
make delivery-cdc-up
make delivery-live
make delivery-streaming-up
make delivery-streaming-check
make delivery-cdc-down
```

`make delivery-down` stops the workload-specific source and cleans up its CDC and
streaming objects. It does not reset the CyberMarket bucket, warehouse, source,
or replication slots.

## Data product

The source includes merchant and courier history, customers, locations,
shipments, lifecycle events, delivery attempts, charges, routes, and stops. Its
fixtures contain late and duplicate events, invalid coordinates, inconsistent
status transitions, missing references, mixed currencies, and malformed contact
fields so transformation and quarantine rules have observable work to do.

Silver publishes conformed SCD2 dimensions, facts, current shipment state, and
`dq_rejected_records`. Gold publishes delivery SLA, hub throughput, courier and
merchant scorecards, failure analysis, route efficiency, margin, and data-quality
metrics. Contracts in `contracts/` define the source and quality expectations.

Every batch writes to a run-specific staging namespace. The quality gate checks
all required tables before the publication pointer advances, so readers continue
to see the previous complete run when any table or check fails.
