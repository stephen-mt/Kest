# Reproducible version lock

All runtime images use a human-readable tag and an immutable manifest digest in
`compose.yaml` or their Dockerfile. Verified stable versions on 2026-10-11:

| Component | Version |
|---|---:|
| Python | 3.12.12 |
| PostgreSQL | 18.6 |
| ClickHouse | 26.9.14.10 stable |
| Redis OSS | 8.10.1 |
| PGSTY SILO (maintained MinIO fork) | RELEASE.2026-09-16T00-00-00Z |
| Airflow | 3.3.2 |
| Superset | 6.1.0 |
| OpenMetadata / ingestion | 2.0.5 / 2.0.5.0 |
| OpenSearch | 3.9.0 |
| dlt | 1.31.0 |
| dbt Core / dbt-clickhouse | 1.12.5 / 1.10.3 |
| Astronomer Cosmos | 1.15.1 |
| clickhouse-connect | 1.10.0 |

Each entry is the newest stable release verified for this platform on the date
above. The floating `latest` tag is deliberately not used in production: the
selected release is pinned by digest so rebuilding the same Git commit cannot
silently install different software. Updating means verifying a newer release,
changing both tag and digest, and rerunning the acceptance suite.
