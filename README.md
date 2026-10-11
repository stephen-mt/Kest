# Kest

Kest is a small-team data platform: DEs build sources and dbt models, DAs work
from governed Gold datasets, and Airflow operates the path between them. The
core runs on one Docker Compose host; OpenMetadata and the sample workload are
optional profiles.

```bash
make up                    # core: Airflow, ClickHouse, SILO, Superset
make credentials           # URLs and local logins
make catalog               # optional OpenMetadata UI plus metadata sync
make de DEVELOPER=alice    # isolated dev_alice database and dbt build
make da                    # Superset entry point for analysts
```

The ecommerce data is only an acceptance fixture. `make demo` loads it; normal
startup does not create or schedule demo workload.

- [What DEs and DAs do every day](docs/workflows.md)
- [Current architecture and security boundaries](docs/architecture.md)
- [Dev, staging, production and CI/CD](docs/environments.md)
- [Operator runbook](docs/operations.md)
- [Pinned component versions](docs/versions.md)
- [Evidence from the last acceptance run](docs/implementation-status.md)
