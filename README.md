# Kest

Kest is a compact OSS data platform for a small data team. Data engineers add
sources and dbt models; analysts discover governed Gold datasets and build
reports without touching infrastructure. Airflow operates the data path,
OpenMetadata carries context and lineage, and Superset is the analyst surface.

![Data platform architecture](assets/platform-architecture.png)

```bash
make up                    # core: Airflow, ClickHouse, SILO, Superset
make credentials           # URLs and local logins
make catalog               # full OpenMetadata UI, lineage and metadata sync
make de DEVELOPER=alice    # isolated dev_alice database and dbt build
make da                    # Superset entry point for analysts
```

The ecommerce data is only an acceptance fixture. `make demo` loads it; normal
startup does not create or schedule demo workload.

## Local product surfaces

| Product | Local URL | Daily use |
| --- | --- | --- |
| Apache Airflow | [http://127.0.0.1:8080](http://127.0.0.1:8080) | Run, schedule and inspect data pipelines |
| Apache Superset | [http://127.0.0.1:8088](http://127.0.0.1:8088) | Explore data and use analyst dashboards |
| SILO Console | [http://127.0.0.1:9001](http://127.0.0.1:9001) | Inspect raw objects and buckets |
| ClickHouse Diagnostics | [http://127.0.0.1:8123/dashboard](http://127.0.0.1:8123/dashboard) | Inspect server health and query activity |
| OpenMetadata | [http://127.0.0.1:8585](http://127.0.0.1:8585) | Find datasets, owners, tests and lineage |

Run `make credentials` to print the local-only usernames and passwords.

The supplied deployment is production-disciplined but single-host. It provides
strong database roles, isolated DE databases, immutable versioned raw data,
tested Gold publishing and repeatable recovery; it does not claim HA or
host-loss disaster recovery.
