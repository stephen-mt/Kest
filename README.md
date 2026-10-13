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
make observability-up      # SLO dashboards, metrics and actionable alerts
make de DEVELOPER=alice    # isolated dev_alice database and dbt build
make da                    # Superset entry point for analysts
```

The ecommerce data is only an acceptance fixture. `make demo` loads it; normal
startup does not create or schedule demo workload.

## Local product surfaces

| Product | Local URL | Primary audience | Daily use |
| --- | --- | --- | --- |
| Apache Airflow | [http://127.0.0.1:8080](http://127.0.0.1:8080) | DE | Run, schedule and inspect data pipelines |
| Apache Superset | [http://127.0.0.1:8088](http://127.0.0.1:8088) | DA | Explore governed data and publish dashboards |
| SILO Console | [http://127.0.0.1:9001](http://127.0.0.1:9001) | DE / platform | Inspect raw objects and buckets |
| ClickHouse Diagnostics | [http://127.0.0.1:8123/dashboard](http://127.0.0.1:8123/dashboard) | DE / platform | Inspect server health and query activity |
| OpenMetadata | [http://127.0.0.1:8585](http://127.0.0.1:8585) | DE / DA | Find datasets, owners, tests and lineage |
| Kest SSO | [http://127.0.0.1:8180](http://127.0.0.1:8180) | Platform | Manage identities and platform roles |
| Grafana | [http://127.0.0.1:3000](http://127.0.0.1:3000) | Platform / DE | Operate SLOs, freshness, releases and capacity |
| Prometheus | [http://127.0.0.1:9090](http://127.0.0.1:9090) | Platform | Inspect metrics and alert rules |
| Alertmanager | [http://127.0.0.1:9093](http://127.0.0.1:9093) | Platform | Inspect, group and silence actionable alerts |

Run `make credentials` to print the local-only usernames and passwords.
Grafana, Prometheus and Alertmanager are enabled by `make observability-up` and
by immutable staging/production deployments. Production deployment also starts
the complete OpenMetadata catalog profile.

The supplied deployment is production-disciplined but single-host. It provides
strong database roles, isolated DE databases, immutable versioned raw data,
tested Gold publishing and repeatable recovery; it does not claim HA or
host-loss disaster recovery.
