# Short workflow

## Operator

Run `make up` once. It creates local secrets, starts the core, applies idempotent
database/object-store bootstrap and prints no demo data. `make credentials`
shows the Airflow, Superset and SILO Console logins. `make health` checks the
control database, ClickHouse and S3 API. `make down` stops containers without
deleting persistent volumes.

OpenMetadata is optional: `make catalog-up` starts it and `make catalog-down`
stops it. The demo is also optional: `make demo` starts the simulator and runs
the sample source-to-dashboard path.

## Data engineer

```bash
make de DEVELOPER=alice
```

This creates `dev_alice`, writes a mode-`0600` local credential file and runs
dbt there. Another engineer uses another name; ClickHouse grants prevent both
from writing Gold. Edit models under `transformations/models` and rerun the same
command.

For a cursor-paginated REST source:

```bash
make source-new SOURCE=crm BASE_URL=https://api.example.com ENDPOINT=/customers \
  PRIMARY_KEY=customer_id
make pipeline
```

The command generates and validates one YAML. Airflow and the loader require no
edit. A known domain table uses its strong raw schema; any new table lands in
append-only `raw.source_records` with batch, timestamp, checksum and record
hash. The DE then promotes JSON fields into a typed dbt staging model. Review
the generated pagination field names when an API does not use the defaults.

## Data analyst

Run `make da`, open Superset and use the curated Gold datasets. Business logic
lives in tested dbt models; dashboards only aggregate canonical columns. The
Superset database identity is read-only in ClickHouse, so a dashboard cannot
accidentally alter production tables.
