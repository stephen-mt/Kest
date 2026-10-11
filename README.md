# Kest

Kest is a compact OSS data platform for a small DE/DA team on one Docker host.
The default stack contains only PostgreSQL, Redis, SILO object storage,
ClickHouse, Airflow and Superset. OpenMetadata and sample workloads are optional
profiles, so they do not consume resources or add operational work unless used.

```bash
make up                         # operator: start the usable platform
make de DEVELOPER=alice         # DE: isolated ClickHouse + dbt workspace
make da                         # DA: Superset URL and login
make source-new SOURCE=crm BASE_URL=https://api.example.com ENDPOINT=/customers
```

`make demo` loads a tiny ecommerce example only when someone wants to test the
whole path. It is not part of normal operation.

Start with [the short workflow](docs/quickstart.md). The production boundary is
documented in [architecture.md](docs/architecture.md), and exact release pins
are in [versions.md](docs/versions.md).
